// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Read logic of instance

use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

use async_stream::try_stream;
use common_types::{
    projected_schema::ProjectedSchema,
    record_batch::{RecordBatch, RecordBatchWithKey},
    schema::RecordSchema,
    time::TimeRange,
};
use common_util::{define_result, error::BoxError};
use futures::stream::Stream;
use log::debug;
use snafu::{ResultExt, Snafu};
use table_engine::{
    stream::{
        self, ErrWithSource, PartitionedStreams, RecordBatchStream, SendableRecordBatchStream,
    },
    table::ReadRequest,
};
use trace_metric::Metric;

use crate::{
    instance::Instance,
    row_iter::{
        chain,
        chain::{ChainConfig, ChainIterator},
        dedup::DedupIterator,
        merge::{MergeBuilder, MergeConfig, MergeIterator},
        IterOptions, RecordBatchWithKeyIterator,
    },
    space::SpaceAndTable,
    sst::factory::{ReadFrequency, SstReadOptions},
    table::{
        data::TableData,
        version::{ReadView, TableVersion},
    },
    table_options::TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to scan memtable, table:{}, err:{}", table, source))]
    ScanMemTable {
        table: String,
        source: crate::memtable::Error,
    },

    #[snafu(display("Failed to build merge iterator, table:{}, err:{}", table, source))]
    BuildMergeIterator {
        table: String,
        source: crate::row_iter::merge::Error,
    },

    #[snafu(display("Failed to build chain iterator, table:{}, err:{}", table, source))]
    BuildChainIterator {
        table: String,
        source: crate::row_iter::chain::Error,
    },
}

define_result!(Error);

const MERGE_SORT_METRIC_NAME: &str = "do_merge_sort";
const ITER_NUM_METRIC_NAME: &str = "iter_num";
const MERGE_ITER_METRICS_COLLECTOR_NAME_PREFIX: &str = "merge_iter";

/// Check whether it needs to apply merge sorting when reading the table with
/// the `table_options` by the `read_request`.
fn need_merge_sort_streams(table_options: &TableOptions, read_request: &ReadRequest) -> bool {
    table_options.need_dedup() || read_request.order.is_in_order()
}

impl Instance {
    /// Read data in multiple time range from table, and return
    /// `read_parallelism` output streams.
    pub async fn partitioned_read_from_table(
        &self,
        space_table: &SpaceAndTable,
        request: ReadRequest,
    ) -> Result<PartitionedStreams> {
        debug!(
            "Instance read from table, space_id:{}, table:{}, table_id:{:?}, request:{:?}",
            space_table.space().id,
            space_table.table_data().name,
            space_table.table_data().id,
            request
        );

        let table_data = space_table.table_data();
        let table_options = table_data.table_options();

        // Collect metrics.
        table_data.metrics.on_read_request_begin();
        let need_merge_sort = need_merge_sort_streams(&table_options, &request);
        request.metrics_collector.collect(Metric::boolean(
            MERGE_SORT_METRIC_NAME.to_string(),
            need_merge_sort,
        ));

        if need_merge_sort {
            let merge_iters = self
                .build_merge_iters(table_data, &request, &table_options)
                .await?;
            self.build_partitioned_streams(&request, merge_iters)
        } else {
            let chain_iters = self
                .build_chain_iters(table_data, &request, &table_options)
                .await?;
            self.build_partitioned_streams(&request, chain_iters)
        }
    }

    fn build_partitioned_streams(
        &self,
        request: &ReadRequest,
        mut partitioned_iters: Vec<impl RecordBatchWithKeyIterator + 'static>,
    ) -> Result<PartitionedStreams> {
        let read_parallelism = request.opts.read_parallelism;

        if read_parallelism == 1 && request.order.is_in_desc_order() {
            // TODO(xikai): it seems this can be avoided.
            partitioned_iters.reverse();
        };

        // Split iterators into `read_parallelism` groups.
        let mut splitted_iters: Vec<_> = std::iter::repeat_with(Vec::new)
            .take(read_parallelism)
            .collect();

        for (i, time_aligned_iter) in partitioned_iters.into_iter().enumerate() {
            splitted_iters[i % read_parallelism].push(time_aligned_iter);
        }

        let mut streams = Vec::with_capacity(read_parallelism);
        for iters in splitted_iters {
            let stream = iters_to_stream(iters, request.projected_schema.clone());
            streams.push(stream);
        }

        assert_eq!(read_parallelism, streams.len());

        Ok(PartitionedStreams { streams })
    }

    async fn build_merge_iters(
        &self,
        table_data: &TableData,
        request: &ReadRequest,
        table_options: &TableOptions,
    ) -> Result<Vec<DedupIterator<MergeIterator>>> {
        // Current visible sequence
        let sequence = table_data.last_sequence();
        let projected_schema = request.projected_schema.clone();
        let sst_read_options = SstReadOptions {
            reverse: request.order.is_in_desc_order(),
            frequency: ReadFrequency::Frequent,
            projected_schema: projected_schema.clone(),
            predicate: request.predicate.clone(),
            meta_cache: self.meta_cache.clone(),
            runtime: self.read_runtime().clone(),
            num_rows_per_row_group: table_options.num_rows_per_row_group,
            scan_options: self.scan_options.clone(),
        };

        let time_range = request.predicate.time_range();
        let version = table_data.current_version();
        let read_views = self.partition_ssts_and_memtables(time_range, version, table_options);
        let iter_options = self.make_iter_options(table_options.num_rows_per_row_group);

        let mut iters = Vec::with_capacity(read_views.len());
        for (idx, read_view) in read_views.into_iter().enumerate() {
            let metrics_collector = request
                .metrics_collector
                .span(format!("{MERGE_ITER_METRICS_COLLECTOR_NAME_PREFIX}_{idx}"));
            let merge_config = MergeConfig {
                request_id: request.request_id,
                metrics_collector: Some(metrics_collector),
                deadline: request.opts.deadline,
                space_id: table_data.space_id,
                table_id: table_data.id,
                sequence,
                projected_schema: projected_schema.clone(),
                predicate: request.predicate.clone(),
                sst_factory: &self.space_store.sst_factory,
                sst_read_options: sst_read_options.clone(),
                store_picker: self.space_store.store_picker(),
                merge_iter_options: iter_options.clone(),
                need_dedup: table_options.need_dedup(),
                reverse: request.order.is_in_desc_order(),
            };

            let merge_iter = MergeBuilder::new(merge_config)
                .sampling_mem(read_view.sampling_mem)
                .memtables(read_view.memtables)
                .ssts_of_level(read_view.leveled_ssts)
                .build()
                .await
                .context(BuildMergeIterator {
                    table: &table_data.name,
                })?;
            let dedup_iter =
                DedupIterator::new(request.request_id, merge_iter, iter_options.clone());

            iters.push(dedup_iter);
        }

        request.metrics_collector.collect(Metric::number(
            ITER_NUM_METRIC_NAME.to_string(),
            iters.len(),
        ));

        Ok(iters)
    }

    async fn build_chain_iters(
        &self,
        table_data: &TableData,
        request: &ReadRequest,
        table_options: &TableOptions,
    ) -> Result<Vec<ChainIterator>> {
        let projected_schema = request.projected_schema.clone();

        assert!(request.order.is_out_of_order());

        let sst_read_options = SstReadOptions {
            // no need to read in order so just read in asc order by default.
            reverse: false,
            frequency: ReadFrequency::Frequent,
            projected_schema: projected_schema.clone(),
            predicate: request.predicate.clone(),
            meta_cache: self.meta_cache.clone(),
            runtime: self.read_runtime().clone(),
            num_rows_per_row_group: table_options.num_rows_per_row_group,
            scan_options: self.scan_options.clone(),
        };

        let time_range = request.predicate.time_range();
        let version = table_data.current_version();
        let read_views = self.partition_ssts_and_memtables(time_range, version, table_options);

        let mut iters = Vec::with_capacity(read_views.len());
        for read_view in read_views {
            let chain_config = ChainConfig {
                request_id: request.request_id,
                deadline: request.opts.deadline,
                space_id: table_data.space_id,
                table_id: table_data.id,
                projected_schema: projected_schema.clone(),
                predicate: request.predicate.clone(),
                sst_read_options: sst_read_options.clone(),
                sst_factory: &self.space_store.sst_factory,
                store_picker: self.space_store.store_picker(),
            };
            let builder = chain::Builder::new(chain_config);
            let chain_iter = builder
                .sampling_mem(read_view.sampling_mem)
                .memtables(read_view.memtables)
                .ssts(read_view.leveled_ssts)
                .build()
                .await
                .context(BuildChainIterator {
                    table: &table_data.name,
                })?;

            iters.push(chain_iter);
        }

        Ok(iters)
    }

    fn partition_ssts_and_memtables(
        &self,
        time_range: TimeRange,
        version: &TableVersion,
        table_options: &TableOptions,
    ) -> Vec<ReadView> {
        let read_view = version.pick_read_view(time_range);

        let segment_duration = match table_options.segment_duration {
            Some(v) => v.0,
            None => {
                // Segment duration is unknown, the table maybe still in sampling phase
                // or the segment duration is still not applied to the table options,
                // just return one partition.
                return vec![read_view];
            }
        };
        if read_view.contains_sampling() {
            // The table contains sampling memtable, just return one partition.
            return vec![read_view];
        }

        // Collect the aligned ssts and memtables into the map.
        // {aligned timestamp} => {read view}
        let mut read_view_by_time = BTreeMap::new();
        for (level, leveled_ssts) in read_view.leveled_ssts.into_iter().enumerate() {
            for file in leveled_ssts {
                let aligned_ts = file
                    .time_range()
                    .inclusive_start()
                    .truncate_by(segment_duration);
                let entry = read_view_by_time
                    .entry(aligned_ts)
                    .or_insert_with(ReadView::default);
                entry.leveled_ssts[level].push(file);
            }
        }

        for memtable in read_view.memtables {
            let aligned_ts = memtable
                .time_range
                .inclusive_start()
                .truncate_by(segment_duration);
            let entry = read_view_by_time
                .entry(aligned_ts)
                .or_insert_with(ReadView::default);
            entry.memtables.push(memtable);
        }

        read_view_by_time.into_values().collect()
    }

    fn make_iter_options(&self, num_rows_per_row_group: usize) -> IterOptions {
        self.iter_options.clone().unwrap_or(IterOptions {
            batch_size: num_rows_per_row_group,
        })
    }
}

struct StreamStateOnMultiIters<I> {
    iters: Vec<I>,
    curr_iter_idx: usize,
    projected_schema: ProjectedSchema,
}

impl<I: RecordBatchWithKeyIterator + 'static> StreamStateOnMultiIters<I> {
    fn is_exhausted(&self) -> bool {
        self.curr_iter_idx >= self.iters.len()
    }

    fn advance(&mut self) {
        self.curr_iter_idx += 1;
    }

    fn curr_iter_mut(&mut self) -> &mut I {
        &mut self.iters[self.curr_iter_idx]
    }

    async fn fetch_next_batch(
        &mut self,
    ) -> Option<std::result::Result<RecordBatchWithKey, I::Error>> {
        loop {
            if self.is_exhausted() {
                return None;
            }

            let iter = self.curr_iter_mut();
            if let Some(v) = iter.next_batch().await.transpose() {
                return Some(v);
            }

            self.advance();
        }
    }
}

fn iters_to_stream(
    iters: Vec<impl RecordBatchWithKeyIterator + 'static>,
    projected_schema: ProjectedSchema,
) -> SendableRecordBatchStream {
    let mut state = StreamStateOnMultiIters {
        projected_schema: projected_schema.clone(),
        iters,
        curr_iter_idx: 0,
    };

    let record_batch_stream = try_stream! {
        while let Some(value) = state.fetch_next_batch().await {
            let record_batch = value
                .box_err()
                .context(ErrWithSource {
                    msg: "Read record batch",
                })
                .and_then(|batch_with_key| {
                    // TODO(yingwen): Try to use projector to do this, which pre-compute row
                    // indexes to project.
                    batch_with_key
                        .try_project(&state.projected_schema)
                        .box_err()
                        .context(ErrWithSource {
                            msg: "Project record batch",
                        })
                });
            yield record_batch?;
        }
    };

    let record_schema = projected_schema.to_record_schema();
    let stream_with_schema = RecordBatchStreamWithSchema {
        schema: record_schema,
        inner_stream: Box::pin(Box::pin(record_batch_stream)),
    };
    Box::pin(stream_with_schema)
}

pub struct RecordBatchStreamWithSchema {
    schema: RecordSchema,
    inner_stream: Pin<Box<dyn Stream<Item = stream::Result<RecordBatch>> + Send + Unpin>>,
}

impl Stream for RecordBatchStreamWithSchema {
    type Item = stream::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner_stream.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for RecordBatchStreamWithSchema {
    fn schema(&self) -> &RecordSchema {
        &self.schema
    }
}
