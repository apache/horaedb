// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Bound, Not},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::BooleanArray,
    datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef},
};
use common_types::{
    projected_schema::ProjectedSchema, record_batch::RecordBatchWithKey, SequenceNumber,
};
use datafusion::{
    common::ToDFSchema,
    error::DataFusionError,
    optimizer::utils::conjunction,
    physical_expr::{self, execution_props::ExecutionProps},
    physical_plan::PhysicalExpr,
};
use futures::stream::{self, Stream, StreamExt};
use generic_error::{BoxError, GenericError};
use macros::define_result;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{predicate::Predicate, table::TableId};
use trace_metric::MetricsCollector;

use crate::{
    memtable::{MemTableRef, ScanContext, ScanRequest},
    space::SpaceId,
    sst::{
        factory::{
            self, FactoryRef as SstFactoryRef, ObjectStorePickerRef, SstReadHint, SstReadOptions,
        },
        file::FileHandle,
    },
    table::sst_util,
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to create sst reader, err:{:?}", source,))]
    CreateSstReader { source: factory::Error },

    #[snafu(display("Fail to read sst meta, err:{}", source))]
    ReadSstMeta { source: crate::sst::reader::Error },

    #[snafu(display("Fail to read sst data, err:{}", source))]
    ReadSstData { source: crate::sst::reader::Error },

    #[snafu(display("Fail to scan memtable, err:{}", source))]
    ScanMemtable { source: crate::memtable::Error },

    #[snafu(display(
        "Fail to execute filter expression, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FilterExec {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Fail to downcast boolean array, actual data type:{:?}.\nBacktrace:\n{}",
        data_type,
        backtrace
    ))]
    DowncastBooleanArray {
        data_type: ArrowDataType,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get datafusion schema, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    DatafusionSchema {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to generate datafusion physical expr, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    DatafusionExpr {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to select from record batch, err:{}", source))]
    SelectBatchData {
        source: common_types::record_batch::Error,
    },

    #[snafu(display(
        "Timeout when read record batch, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    Timeout {
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    },
}

define_result!(Error);

// TODO(yingwen): Can we move sequence to RecordBatchWithKey and remove this
// struct? But what is the sequence after merge?
#[derive(Debug)]
pub struct SequencedRecordBatch {
    pub record_batch: RecordBatchWithKey,
    pub sequence: SequenceNumber,
}

impl SequencedRecordBatch {
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.record_batch.num_rows()
    }
}

pub type SequencedRecordBatchStream =
    Box<dyn Stream<Item = std::result::Result<SequencedRecordBatch, GenericError>> + Send + Unpin>;

/// Filter the `sequenced_record_batch` according to the `predicate`.
fn filter_record_batch(
    mut sequenced_record_batch: SequencedRecordBatch,
    predicate: Arc<dyn PhysicalExpr>,
) -> Result<Option<SequencedRecordBatch>> {
    let record_batch = sequenced_record_batch.record_batch.as_arrow_record_batch();
    let filter_array = predicate
        .evaluate(record_batch)
        .map(|v| v.into_array(record_batch.num_rows()))
        .context(FilterExec)?;
    let selected_rows = filter_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context(DowncastBooleanArray {
            data_type: filter_array.as_ref().data_type().clone(),
        })?;

    sequenced_record_batch
        .record_batch
        .select_data(selected_rows)
        .context(SelectBatchData)?;

    sequenced_record_batch
        .record_batch
        .is_empty()
        .not()
        .then_some(Ok(sequenced_record_batch))
        .transpose()
}

/// Filter the sequenced record batch stream by applying the `predicate`.
pub fn filter_stream(
    origin_stream: SequencedRecordBatchStream,
    input_schema: ArrowSchemaRef,
    predicate: &Predicate,
) -> Result<SequencedRecordBatchStream> {
    let filter = match conjunction(predicate.exprs().to_owned()) {
        Some(filter) => filter,
        None => return Ok(origin_stream),
    };

    let input_df_schema = input_schema
        .clone()
        .to_dfschema()
        .context(DatafusionSchema)?;
    let execution_props = ExecutionProps::new();
    let predicate = physical_expr::create_physical_expr(
        &filter,
        &input_df_schema,
        input_schema.as_ref(),
        &execution_props,
    )
    .context(DatafusionExpr)?;

    let stream = origin_stream.filter_map(move |sequence_record_batch| {
        let v = match sequence_record_batch {
            Ok(v) => filter_record_batch(v, predicate.clone())
                .box_err()
                .transpose(),
            Err(e) => Some(Err(e)),
        };

        futures::future::ready(v)
    });

    Ok(Box::new(stream))
}

/// Build filtered (by `predicate`) [SequencedRecordBatchStream] from a
/// memtable.
pub fn filtered_stream_from_memtable(
    projected_schema: ProjectedSchema,
    need_dedup: bool,
    memtable: &MemTableRef,
    reverse: bool,
    predicate: &Predicate,
    deadline: Option<Instant>,
    metrics_collector: Option<MetricsCollector>,
) -> Result<SequencedRecordBatchStream> {
    stream_from_memtable(
        projected_schema.clone(),
        need_dedup,
        memtable,
        reverse,
        deadline,
        metrics_collector,
    )
    .and_then(|origin_stream| {
        filter_stream(
            origin_stream,
            projected_schema
                .as_record_schema_with_key()
                .to_arrow_schema_ref(),
            predicate,
        )
    })
}

/// Build [SequencedRecordBatchStream] from a memtable.
pub fn stream_from_memtable(
    projected_schema: ProjectedSchema,
    need_dedup: bool,
    memtable: &MemTableRef,
    reverse: bool,
    deadline: Option<Instant>,
    metrics_collector: Option<MetricsCollector>,
) -> Result<SequencedRecordBatchStream> {
    let scan_ctx = ScanContext {
        deadline,
        ..Default::default()
    };
    let max_seq = memtable.last_sequence();
    let scan_memtable_desc = format!("scan_memtable_{max_seq}");
    let metrics_collector = metrics_collector.map(|v| v.span(scan_memtable_desc));
    let scan_req = ScanRequest {
        start_user_key: Bound::Unbounded,
        end_user_key: Bound::Unbounded,
        sequence: max_seq,
        projected_schema,
        need_dedup,
        reverse,
        metrics_collector,
    };

    let iter = memtable.scan(scan_ctx, scan_req).context(ScanMemtable)?;
    let stream = stream::iter(iter).map(move |v| {
        v.map(|record_batch| SequencedRecordBatch {
            record_batch,
            sequence: max_seq,
        })
        .box_err()
    });

    Ok(Box::new(stream))
}

/// Build the filtered by `sst_read_options.predicate`
/// [SequencedRecordBatchStream] from a sst.
pub async fn filtered_stream_from_sst_file(
    space_id: SpaceId,
    table_id: TableId,
    sst_file: &FileHandle,
    sst_factory: &SstFactoryRef,
    sst_read_options: &SstReadOptions,
    store_picker: &ObjectStorePickerRef,
    metrics_collector: Option<MetricsCollector>,
) -> Result<SequencedRecordBatchStream> {
    stream_from_sst_file(
        space_id,
        table_id,
        sst_file,
        sst_factory,
        sst_read_options,
        store_picker,
        metrics_collector,
    )
    .await
    .and_then(|origin_stream| {
        filter_stream(
            origin_stream,
            sst_read_options
                .projected_schema
                .as_record_schema_with_key()
                .to_arrow_schema_ref(),
            sst_read_options.predicate.as_ref(),
        )
    })
}

/// Build the [SequencedRecordBatchStream] from a sst.
pub async fn stream_from_sst_file(
    space_id: SpaceId,
    table_id: TableId,
    sst_file: &FileHandle,
    sst_factory: &SstFactoryRef,
    sst_read_options: &SstReadOptions,
    store_picker: &ObjectStorePickerRef,
    metrics_collector: Option<MetricsCollector>,
) -> Result<SequencedRecordBatchStream> {
    sst_file.read_meter().mark();
    let path = sst_util::new_sst_file_path(space_id, table_id, sst_file.id());

    let read_hint = SstReadHint {
        file_size: Some(sst_file.size() as usize),
        file_format: Some(sst_file.storage_format()),
    };
    let scan_sst_desc = format!("scan_sst_{}", sst_file.id());
    let metrics_collector = metrics_collector.map(|v| v.span(scan_sst_desc));
    let mut sst_reader = sst_factory
        .create_reader(
            &path,
            sst_read_options,
            read_hint,
            store_picker,
            metrics_collector,
        )
        .await
        .context(CreateSstReader)?;
    let meta = sst_reader.meta_data().await.context(ReadSstMeta)?;
    let max_seq = meta.max_sequence();
    let sst_stream = sst_reader.read().await.context(ReadSstData)?;

    let stream = Box::new(sst_stream.map(move |v| {
        v.map(|record_batch| SequencedRecordBatch {
            record_batch,
            sequence: max_seq,
        })
        .box_err()
    }));

    Ok(stream)
}

#[cfg(test)]
pub mod tests {
    use common_types::{row::Row, schema::Schema};

    use super::*;
    use crate::row_iter;

    /// Build [SequencedRecordBatchStream] from the sequenced rows.
    pub fn build_sequenced_record_batch_stream(
        schema: &Schema,
        batches: Vec<(SequenceNumber, Vec<Row>)>,
    ) -> Vec<SequencedRecordBatchStream> {
        batches
            .into_iter()
            .map(|(seq, rows)| {
                let batch = SequencedRecordBatch {
                    record_batch: row_iter::tests::build_record_batch_with_key(
                        schema.clone(),
                        rows,
                    ),
                    sequence: seq,
                };
                Box::new(stream::iter(vec![Ok(batch)])) as SequencedRecordBatchStream
            })
            .collect()
    }
}
