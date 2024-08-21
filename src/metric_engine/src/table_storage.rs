use wal::manager::WalManagerRef;

pub struct SSTable {
    id: u64,
}

/// Storage to represent different components of the system.
/// Such as: metrics, series, indexes, etc.
///
/// Columns for design:
/// metrics: {MetricName}-{MetricID}-{FieldName}
/// series: {TSID}-{SeriesKey}
/// index: {TagKey}-{TagValue}-{TSID}
pub struct TableStorage {
    name: String,
    id: u64,
    wal: WalManagerRef,
    sstables: Vec<SSTable>,
}
