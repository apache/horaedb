// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    collections::{hash_map, BTreeMap, HashMap, VecDeque},
    fmt, mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{new_empty_array, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array},
    compute::concat_batches,
    error::ArrowError,
    record_batch::RecordBatch,
};
use common_types::{
    schema::{ArrowSchema, ArrowSchemaRef, DataType, TSID_COLUMN},
    time::{TimeRange, Timestamp},
};
use datafusion::{
    error::{DataFusionError, Result as ArrowResult},
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        repartition::RepartitionExec, ColumnarValue, DisplayFormatType, ExecutionPlan,
        Partitioning, PhysicalExpr, RecordBatchStream,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use futures::{Stream, StreamExt};
use log::debug;
use snafu::{OptionExt, ResultExt, Snafu};
use sql::promql::{AlignParameter, ColumnNames, Func as PromFunc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal err, source:{:?}", source))]
    Internal { source: DataFusionError },

    #[snafu(display("Invalid schema, source:{:?}", source))]
    InvalidSchema { source: common_types::schema::Error },

    #[snafu(display("Tsid column is required"))]
    TsidRequired,

    #[snafu(display("Invalid column type, required:{:?}", required_type))]
    InvalidColumnType { required_type: String },

    #[snafu(display("{} column type cannot be null", name))]
    NullColumn { name: String },

    #[snafu(display("timestamp out of range"))]
    TimestampOutOfRange {},
}

define_result!(Error);

/// Limits Extrapolation range.
/// Refer to https://github.com/prometheus/prometheus/pull/1295
const PROMETHEUS_EXTRAPOLATION_THRESHOLD_COEFFICIENT: f64 = 1.1;

#[derive(Debug)]
struct ExtractTsidExpr {}

impl fmt::Display for ExtractTsidExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(ExtractTsid)")
    }
}

impl PartialEq<dyn Any> for ExtractTsidExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other).downcast_ref::<Self>().is_some()
    }
}

// Copy from https://github.com/apache/arrow-datafusion/blob/71353bb9ad99a0688a9ae36a5cda77a5ab6af00b/datafusion/physical-expr/src/physical_expr.rs#L237
fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

impl PhysicalExpr for ExtractTsidExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &ArrowSchema) -> ArrowResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> ArrowResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> ArrowResult<ColumnarValue> {
        let tsid_idx = batch
            .schema()
            .index_of(TSID_COLUMN)
            .expect("checked in plan build");
        Ok(ColumnarValue::Array(batch.column(tsid_idx).clone()))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> ArrowResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

/// Note: caller should ensure data[tail_index] is valid
pub(crate) trait AlignFunc: fmt::Debug {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        param: &AlignParameter,
    ) -> Result<Option<Sample>>;
}

/// PromAlignExec will group data by tsid and align sample based on align_param
#[derive(Debug)]
pub struct PromAlignExec {
    input: Arc<dyn ExecutionPlan>,
    column_name: Arc<ColumnNames>,
    align_func: Arc<dyn AlignFunc + Send + Sync>,
    align_param: AlignParameter,
}

impl PromAlignExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: Arc<ColumnNames>,
        func: PromFunc,
        align_param: AlignParameter,
        read_parallelism: usize,
    ) -> Result<Self> {
        let extract_tsid: Arc<dyn PhysicalExpr> = Arc::new(ExtractTsidExpr {});
        let input = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![extract_tsid], read_parallelism),
            )
            .context(Internal)?,
        ) as Arc<dyn ExecutionPlan>;
        let align_func: Arc<dyn AlignFunc + Send + Sync> = match func {
            PromFunc::Instant => Arc::new(InstantFunc {}),
            PromFunc::Rate => Arc::new(RateFunc {}),
            PromFunc::Irate => Arc::new(IrateFunc {}),
            PromFunc::Delta => Arc::new(DeltaFunc {}),
            PromFunc::Idelta => Arc::new(IdeltaFunc {}),
            PromFunc::Increase => Arc::new(IncreaseFunc {}),
        };
        Ok(Self {
            input,
            column_name,
            align_func,
            align_param,
        })
    }
}

impl ExecutionPlan for PromAlignExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> ArrowResult<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(PromAlignExec {
                input: children[0].clone(),
                column_name: self.column_name.clone(),
                align_func: self.align_func.clone(),
                align_param: self.align_param,
            })),
            _ => Err(DataFusionError::Internal(
                "PromAlignExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> ArrowResult<DfSendableRecordBatchStream> {
        debug!("PromAlignExec: partition:{}", partition);
        Ok(Box::pin(PromAlignReader {
            input: self.input.execute(partition, context)?,
            done: false,
            column_name: self.column_name.clone(),
            align_func: self.align_func.clone(),
            align_param: self.align_param,
            tsid_to_tags: HashMap::default(),
            tsid_to_stepper: HashMap::default(),
            record_schema: None,
        }))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PromAlignExec: align_param={:?}, func={:?}, partition_count={}",
            self.align_param,
            self.align_func,
            self.output_partitioning().partition_count(),
        )
    }

    fn statistics(&self) -> Statistics {
        // TODO(chenxiang)
        Statistics::default()
    }
}

struct PromAlignReader {
    /// The input to read data from
    input: DfSendableRecordBatchStream,
    /// Have we produced the output yet?
    done: bool,
    column_name: Arc<ColumnNames>,
    align_func: Arc<dyn AlignFunc + Send + Sync>,
    align_param: AlignParameter,
    tsid_to_tags: HashMap<u64, BTreeMap<String, String>>,
    tsid_to_stepper: HashMap<u64, Box<dyn Stepper + Send + Sync>>,
    record_schema: Option<ArrowSchemaRef>,
}

impl PromAlignReader {
    fn step_helper(&mut self, tsid: u64, samples: Vec<Sample>) -> Result<Option<Vec<Sample>>> {
        let start_timestamp = self.align_param.align_range.inclusive_start();
        let offset = self.align_param.offset;
        let stepper = self.tsid_to_stepper.entry(tsid).or_insert_with(|| {
            Box::new(FixedStepper::new(start_timestamp)) as Box<dyn Stepper + Send + Sync>
        });
        let samples = samples
            .into_iter()
            .map(|Sample { timestamp, value }| {
                Ok(Sample {
                    timestamp: timestamp
                        .checked_add(offset)
                        .context(TimestampOutOfRange {})?,
                    value,
                })
            })
            .collect::<Result<VecDeque<_>>>()?;
        let sample_range = if samples.is_empty() {
            TimeRange::min_to_max()
        } else {
            TimeRange::new_unchecked(
                samples.front().unwrap().timestamp, // we have at least one samples here
                samples
                    .back()
                    .unwrap()
                    .timestamp
                    .checked_add_i64(1)
                    .context(TimestampOutOfRange {})?,
            )
        };
        stepper.step(
            samples,
            sample_range,
            &self.align_param,
            self.align_func.clone(),
        )
    }

    fn accumulate_record_batch(
        &mut self,
        record_batch: RecordBatch,
    ) -> Result<HashMap<u64, Vec<Sample>>> {
        let schema = record_batch.schema();
        let tsid_idx = schema.index_of(TSID_COLUMN).expect("checked in plan build");
        let field_idx = schema
            .index_of(&self.column_name.field)
            .expect("checked in plan build");
        let timestamp_idx = schema
            .index_of(&self.column_name.timestamp)
            .expect("checked in plan build");

        let mut tsid_samples = HashMap::new();
        let tsid_array = record_batch
            .column(tsid_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked in build plan");
        if tsid_array.is_empty() {
            // empty array means end of data, but maybe there are still pending samples, so
            // step one more time
            let tsids = self.tsid_to_stepper.keys().cloned().collect::<Vec<_>>();
            for tsid in tsids {
                if let Some(result) = self.step_helper(tsid, vec![])? {
                    tsid_samples.insert(tsid, result);
                }
            }
            return Ok(tsid_samples);
        }

        let mut previous_tsid = tsid_array.value(0);
        let mut duplicated_tsids = vec![(previous_tsid, 0)];
        for row_idx in 1..tsid_array.len() {
            let tsid = tsid_array.value(row_idx);
            if tsid != previous_tsid {
                previous_tsid = tsid;
                duplicated_tsids.push((tsid, row_idx));
            }
        }
        let mut step_helper = |tsid, batch| {
            if let hash_map::Entry::Vacant(e) = self.tsid_to_tags.entry(tsid) {
                e.insert(Self::build_tags(
                    &self.column_name.tag_keys,
                    schema.clone(),
                    &batch,
                )?);
            }
            if let Some(result) =
                self.step_helper(tsid, self.build_sample(field_idx, timestamp_idx, batch)?)?
            {
                tsid_samples.insert(tsid, result);
            }
            Ok(())
        };
        if duplicated_tsids.len() == 1 {
            // fast path, when there is only one tsid in record_batch
            step_helper(duplicated_tsids[0].0, record_batch)?;
        } else {
            debug!("duplicated_tsids:{:?}", duplicated_tsids);
            for i in 0..duplicated_tsids.len() {
                let (tsid, offset) = duplicated_tsids[i];
                let length = if i == duplicated_tsids.len() - 1 {
                    tsid_array.len() - offset
                } else {
                    duplicated_tsids[i + 1].1 - offset
                };
                let current_batch = record_batch.slice(offset, length);
                step_helper(tsid, current_batch)?;
            }
        }

        Ok(tsid_samples)
    }

    fn build_tags(
        tag_keys: &[String],
        schema: ArrowSchemaRef,
        record_batch: &RecordBatch,
    ) -> Result<BTreeMap<String, String>> {
        tag_keys
            .iter()
            .map(|key| {
                let v = record_batch
                    .column(schema.index_of(key).expect("checked in build plan"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context(InvalidColumnType {
                        required_type: "StringArray",
                    })?
                    .value(0);
                Ok((key.to_owned(), v.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>>>()
    }

    fn build_sample(
        &self,
        field_idx: usize,
        timestamp_idx: usize,
        record_batch: RecordBatch,
    ) -> Result<Vec<Sample>> {
        let field_array = record_batch
            .column(field_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context(InvalidColumnType {
                required_type: "Float64Array",
            })?;
        let timestamp_array = record_batch
            .column(timestamp_idx)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context(InvalidColumnType {
                required_type: "TimestampMillisecondArray",
            })?;
        field_array
            .into_iter()
            .zip(timestamp_array.into_iter())
            .map(|(field, timestamp)| {
                Ok(Sample {
                    value: field.context(NullColumn { name: "field" })?,
                    timestamp: Timestamp::new(timestamp.context(NullColumn { name: "timestamp" })?),
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn samples_to_record_batch(
        &self,
        schema: ArrowSchemaRef,
        tsid_samples: HashMap<u64, Vec<Sample>>,
    ) -> std::result::Result<RecordBatch, ArrowError> {
        let tsid_idx = schema.index_of(TSID_COLUMN).expect("checked in plan build");
        let field_idx = schema
            .index_of(&self.column_name.field)
            .expect("checked in plan build");
        let timestamp_idx = schema
            .index_of(&self.column_name.timestamp)
            .expect("checked in plan build");
        let mut batches = Vec::with_capacity(tsid_samples.len());
        for (tsid, samples) in tsid_samples {
            let record_batch_len = samples.len();
            let tags = self
                .tsid_to_tags
                .get(&tsid)
                .expect("tags are ensured in accumulated_record_batch");
            let mut arrays = vec![new_empty_array(&DataType::Int32); schema.fields().len()];
            arrays[tsid_idx] = Arc::new(UInt64Array::from(vec![tsid; record_batch_len]));
            let mut fields = Vec::with_capacity(record_batch_len);
            let mut timestamps = Vec::with_capacity(record_batch_len);
            for Sample {
                timestamp,
                value: field,
            } in samples
            {
                fields.push(field);
                timestamps.push(timestamp.as_i64());
            }
            arrays[timestamp_idx] = Arc::new(TimestampMillisecondArray::from(timestamps));
            arrays[field_idx] = Arc::new(Float64Array::from(fields));

            for tag_key in &self.column_name.tag_keys {
                let tag_idx = schema
                    .index_of(tag_key.as_str())
                    .expect("checked in plan build");
                arrays[tag_idx] = Arc::new(StringArray::from(vec![
                    tags.get(tag_key)
                        .expect("tag_key are ensured in accmulate_record_batch")
                        .to_string();
                    record_batch_len
                ]));
            }
            batches.push(RecordBatch::try_new(schema.clone(), arrays)?);
        }

        concat_batches(&schema, &batches)
    }
}

impl Stream for PromAlignReader {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let schema = batch.schema();
                if self.record_schema.is_none() {
                    self.record_schema = Some(schema.clone());
                }
                let tsid_samples = self
                    .accumulate_record_batch(batch)
                    .map_err(|e| DataFusionError::External(Box::new(e) as Box<_>))?; // convert all Error enum to SchemaError
                if !tsid_samples.is_empty() {
                    Poll::Ready(Some(
                        self.samples_to_record_batch(schema, tsid_samples)
                            .map_err(DataFusionError::ArrowError),
                    ))
                } else {
                    Poll::Ready(Some(Ok(RecordBatch::new_empty(schema))))
                }
            }
            Poll::Ready(None) => {
                self.done = true;
                if let Some(schema) = mem::take(&mut self.record_schema) {
                    let tsid_samples = self
                        .accumulate_record_batch(RecordBatch::new_empty(schema.clone()))
                        .map_err(|e| DataFusionError::External(Box::new(e) as Box<_>))?;
                    if !tsid_samples.is_empty() {
                        return Poll::Ready(Some(
                            self.samples_to_record_batch(schema, tsid_samples)
                                .map_err(DataFusionError::ArrowError),
                        ));
                    }
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl RecordBatchStream for PromAlignReader {
    fn schema(&self) -> ArrowSchemaRef {
        self.input.schema()
    }
}

#[derive(Debug)]
pub(crate) struct Sample {
    timestamp: Timestamp,
    value: f64,
}

/// `Stepper` is used for align samples, specified by [range queries](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries).
/// Note: [instant queries](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries) are models as range queries with step is 1.
///
/// # Diagram
/// ```plaintext
///                        range
///                   +-------------+
///                   v             |
///  |------|-----|-----|-----|-----|-------->
/// start                 step               end
/// ```
trait Stepper: fmt::Debug {
    /// Calculate current sample based on new input samples.
    /// Samples maybe kept since some function require large time range input,
    /// such as rate(metric[1d])
    fn step(
        &mut self,
        input: VecDeque<Sample>,
        range: TimeRange,
        param: &AlignParameter,
        align_func: Arc<dyn AlignFunc + Send + Sync>,
    ) -> Result<Option<Vec<Sample>>>;

    // Returns size of samples kept during query, mainly used for metrics
    fn pending_column_bytes(&self) -> usize;
}

/// `FixedStepper` is one implemention of `Stepper`, which will accumulate all
/// samples within each step before pass control to next execution node.
/// This implemention will consume high memory in large range query, such as
/// rate(metric[30d])

/// TODO(chenxiang): A streaming implemention is required for those large range
/// query.
#[derive(Debug)]
struct FixedStepper {
    /// accumulated samples used for calculate sample for current step
    entries: VecDeque<Sample>,
    /// tail index of entries for processing current step, which means
    /// [0, tail_index] is used
    tail_index: usize,
    /// timestamp of current step sample
    timestamp: Timestamp,
}

impl Stepper for FixedStepper {
    fn step(
        &mut self,
        mut column: VecDeque<Sample>,
        column_range: TimeRange,
        param: &AlignParameter,
        align_func: Arc<dyn AlignFunc + Send + Sync>,
    ) -> Result<Option<Vec<Sample>>> {
        self.entries.append(&mut column);
        debug!(
            "column_range:{:?}, param:{:?}, ts:{:?}",
            column_range, param, self.timestamp
        );
        let curr_range = param.align_range.intersected_range(column_range);
        if curr_range.is_none() {
            return Ok(None);
        }
        let curr_range = curr_range.unwrap();
        let mut result = vec![];

        // self.timestamp = self.timestamp.max(start);
        while self.timestamp < curr_range.inclusive_start() {
            self.timestamp = self
                .timestamp
                .checked_add(param.step)
                .context(TimestampOutOfRange {})?;
        }

        while curr_range.contains(self.timestamp) {
            // push `tail_index`. In look ahead (by increasing index by 1) way.
            while self.tail_index + 1 < self.entries.len()
                && self.entries[self.tail_index + 1].timestamp <= self.timestamp
            {
                self.tail_index += 1;
            }
            let mint = self
                .timestamp
                .checked_sub(param.lookback_delta)
                .context(TimestampOutOfRange {})?;
            // drop some unneeded entries from begining of `entries`
            while let Some(entry) = self.entries.front() {
                if entry.timestamp < mint {
                    self.entries.pop_front();
                    if let Some(index) = self.tail_index.checked_sub(1) {
                        self.tail_index = index
                    }
                } else {
                    break;
                }
            }
            // [mint, self.timestamp] has no data, skip to next step.
            let skip = {
                if let Some(first_entry) = self.entries.get(0) {
                    first_entry.timestamp > self.timestamp
                } else {
                    true
                }
            };
            if skip {
                self.timestamp = self
                    .timestamp
                    .checked_add(param.step)
                    .context(TimestampOutOfRange {})?;
                continue;
            }

            // call range function
            if let Some(value) =
                align_func.call(&self.entries, self.tail_index, self.timestamp, param)?
            {
                result.push(value);
            }

            self.timestamp = self
                .timestamp
                .checked_add(param.step)
                .context(TimestampOutOfRange {})?;
        }

        if !result.is_empty() {
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    fn pending_column_bytes(&self) -> usize {
        self.entries.len() * 16 // timestamp + float value
    }
}

impl FixedStepper {
    fn new(start_timestamp: Timestamp) -> FixedStepper {
        Self {
            entries: VecDeque::new(),
            tail_index: 0,
            timestamp: start_timestamp,
        }
    }
}

/// Helper for Promtheus functions which needs extrapolation. [Rate][rate],
/// [Increase][increase] and [Delta][delta] for now.
///
/// Since "range" is not always equals to `data_duration`, extrapolation needs
/// to be performed to estimate absent data. Extrapolation is named by
/// Prometheus. This function is ported from [here][prom_extrapolate_code].
/// "extrapolate" assumes absenting data is following the same distribution with
/// existing data. Thus it simply zooms result calculated from existing data to
/// required extrapolation time range.
///
/// [rate]: https://prometheus.io/docs/prometheus/latest/querying/functions/#rate
/// [increase]: https://prometheus.io/docs/prometheus/latest/querying/functions/#increase
/// [delta]: https://prometheus.io/docs/prometheus/latest/querying/functions/#delta
/// [prom_extrapolate_code]: https://github.com/prometheus/prometheus/blob/063154eab720d8c3d495bd78312c0df090d0bf23/promql/functions.go#L59
///
/// This function can be roughly divided into three parts:
/// - Calculate result from real data
/// - Calculate time range needs extrapolate to.
/// - Calculate extrapolated result.
///
/// The outputs of above three steps are `difference`, `extrapolated_duration`
/// and `extrapolated_result`.
///
/// # Diagram
/// ```plaintext
/// range_start        first_timestamp       last_timestamp       range_end
///      └─────────────────────┴────────────────────┴──────────────────┘
///          range_to_start        data_duration         range_to_end
/// ```
///
/// Legends:
/// - `range_end` is the timestamp passed in
/// - `range_start` is calculated by `timestamp` - `lookback_range`.
/// - "range" here stands for `range_end` - `range_start`, which is equals to
///   `range_to_start` + `data_duration` + `range_to_end`.
/// - `first/last_timestamp` is the timestamp of provided data.
/// - `data_duration` is a time range covered by data.
fn extrapolate_fn_helper(
    data: &VecDeque<Sample>,
    tail_index: usize,
    timestamp: Timestamp,
    lookback_delta: Timestamp,
    is_counter: bool,
    is_rate: bool,
) -> Result<Option<Sample>> {
    // no sence to calculate rate on one single item.
    if tail_index < 1 {
        return Ok(None);
    }

    let first_data = data[0].value;

    // calculate `counter_reset_correction` for counter type.
    let mut counter_reset_correction = 0.0;
    if is_counter {
        let mut last_data = first_data;
        for Sample { value, .. } in data.iter().take(tail_index + 1) {
            if *value < last_data {
                counter_reset_correction += last_data;
            }
            last_data = *value;
        }
    }

    let difference = data[tail_index].value - first_data + counter_reset_correction;

    // `average_duration_between_data` assumes all data is distributed evenly.
    let first_timestamp = data[0].timestamp;
    let last_timestamp = data[tail_index].timestamp;
    let data_duration = (last_timestamp
        .checked_sub(first_timestamp)
        .context(TimestampOutOfRange {})?)
    .as_i64() as f64;
    let average_duration_between_data = data_duration / tail_index as f64;

    let range_start = timestamp
        .checked_sub(lookback_delta)
        .context(TimestampOutOfRange {})?;
    let range_end = timestamp;
    let mut range_to_start = (first_timestamp
        .checked_sub(range_start)
        .context(TimestampOutOfRange)?)
    .as_i64() as f64;
    let mut range_to_end = (range_end
        .checked_sub(last_timestamp)
        .context(TimestampOutOfRange {})?)
    .as_i64() as f64;

    // Prometheus shorten forward-extrapolation to zero point.
    if is_counter && difference > 0.0 && first_data >= 0.0 {
        let range_to_zero_point = data_duration * (first_data / difference);
        range_to_start = range_to_start.min(range_to_zero_point);
    }

    let extrapolation_threshold =
        average_duration_between_data * PROMETHEUS_EXTRAPOLATION_THRESHOLD_COEFFICIENT;

    // if lots of data is absent (`range_to_start` or `range_to_end` is longer than
    // `extrapolation_threshold`), Prometheus will not estimate all time range. Use
    // half of `average_duration_between_data` instead.
    if range_to_start > extrapolation_threshold {
        range_to_start = average_duration_between_data / 2.0;
    }
    if range_to_end > extrapolation_threshold {
        range_to_end = average_duration_between_data / 2.0;
    }

    // `difference` is the real result calculated by existing data. Prometheus will
    // zoom it to `extrapolated_duration` to get extrapolated estimated result.
    let extrapolated_duration = data_duration + range_to_start + range_to_end;
    let mut extrapolated_result = difference * extrapolated_duration / data_duration;

    if is_rate {
        // `lookback_delta` here is in millisecond.
        extrapolated_result /= lookback_delta.as_i64() as f64 / 1000.0;
    }

    Ok(Some(Sample {
        timestamp,
        value: extrapolated_result,
    }))
}

/// Implementation of `Rate` function in `Prometheus`. More
/// [details](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate)
#[derive(Debug)]
struct RateFunc {}

impl AlignFunc for RateFunc {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        param: &AlignParameter,
    ) -> Result<Option<Sample>> {
        extrapolate_fn_helper(
            data,
            tail_index,
            timestamp,
            param.lookback_delta,
            true,
            true,
        )
    }
}

#[derive(Debug)]
struct DeltaFunc {}

impl AlignFunc for DeltaFunc {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        param: &AlignParameter,
    ) -> Result<Option<Sample>> {
        extrapolate_fn_helper(
            data,
            tail_index,
            timestamp,
            param.lookback_delta,
            false,
            false,
        )
    }
}

#[derive(Debug)]
struct IncreaseFunc {}

impl AlignFunc for IncreaseFunc {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        param: &AlignParameter,
    ) -> Result<Option<Sample>> {
        extrapolate_fn_helper(
            data,
            tail_index,
            timestamp,
            param.lookback_delta,
            true,
            false,
        )
    }
}

// Port from https://github.com/prometheus/prometheus/blob/063154eab720d8c3d495bd78312c0df090d0bf23/promql/functions.go#L159
fn instant_value(
    data: &VecDeque<Sample>,
    tail_index: usize,
    timestamp: Timestamp,
    is_rate: bool,
) -> Result<Option<Sample>> {
    if tail_index < 2 {
        return Ok(None);
    }

    let last_entry = &data[tail_index];
    let previous_entry = &data[tail_index - 1];

    let mut result = if is_rate && last_entry.value < previous_entry.value {
        last_entry.value
    } else {
        last_entry.value - previous_entry.value
    };

    let interval = last_entry
        .timestamp
        .checked_sub(previous_entry.timestamp)
        .context(TimestampOutOfRange {})?;
    assert!(interval.as_i64() > 0);

    if is_rate {
        // Convert to per-second.
        result /= interval.as_i64() as f64 / 1000.0;
    }

    Ok(Some(Sample {
        value: result,
        timestamp,
    }))
}

#[derive(Debug)]
pub struct IdeltaFunc;

impl AlignFunc for IdeltaFunc {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        _param: &AlignParameter,
    ) -> Result<Option<Sample>> {
        instant_value(data, tail_index, timestamp, false)
    }
}

#[derive(Debug)]
struct IrateFunc;

impl AlignFunc for IrateFunc {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        _param: &AlignParameter,
    ) -> Result<Option<Sample>> {
        instant_value(data, tail_index, timestamp, true)
    }
}

/// This function is not in Promtheus' functions list.
///
/// It simulates the behavior of `Instant Selector` by finding the newest point
/// from the input. Thus `Instant Selector` can be represented by [PromAlignOp]
/// + [InstantFn].
#[derive(Debug)]
pub struct InstantFunc;

impl AlignFunc for InstantFunc {
    fn call(
        &self,
        data: &VecDeque<Sample>,
        tail_index: usize,
        timestamp: Timestamp,
        _param: &AlignParameter,
    ) -> Result<Option<Sample>> {
        Ok(Some(Sample {
            timestamp,
            value: data[tail_index].value,
        }))
    }
}
