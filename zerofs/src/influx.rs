use std::path::PathBuf;
use std::sync::Arc;
use slatedb_common::metrics::DefaultMetricsRecorder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use crate::dedup::DedupCache;
use crate::fs::metrics::{FileSystemStats, SegmentReclaimStats};
use crate::fs::stats::FileSystemGlobalStats;

pub fn start(
    config_phat: PathBuf,
    fs_stats: Arc<FileSystemStats>,
    global_stats: Arc<FileSystemGlobalStats>,
    segment_reclaim_stats: Arc<SegmentReclaimStats>,
    dedup_cache: Arc<DedupCache>,
    default_metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    shutdown: CancellationToken
) -> Vec<JoinHandle<()>> {
    todo!()
}