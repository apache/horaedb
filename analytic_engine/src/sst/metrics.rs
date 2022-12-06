use lazy_static::lazy_static;
use prometheus::{register_histogram, Histogram};
lazy_static! {
    // Histograms:
    // Buckets: 100B,500B,1KB,100KB,500KB,1M,5M
    pub static ref SST_GET_RANGE_HISTOGRAM: Histogram = register_histogram!(
        "sst_get_range_length",
        "Histogram for sst get range length",
        vec!(100.0, 500.0, 1024.0, 1024.0 * 5.0, 1024.0 * 100.0, 1024.0 * 1000.0 * 5.0, 1024.0 * 1000.0 * 1000.0, 1024.0 * 1000.0 * 1000.0 * 5.0)
    ).unwrap();
}
