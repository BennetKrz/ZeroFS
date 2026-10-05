use std::path::PathBuf;
use std::sync::Arc;
use slatedb_common::metrics::DefaultMetricsRecorder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use crate::dedup::DedupCache;
use crate::fs::metrics::{FileSystemStats, SegmentReclaimStats};
use crate::fs::stats::FileSystemGlobalStats;

use crate::config::InfluxConfig;
use crate::fs::ZeroFS;

struct InfluxExporter {
    fs: Arc<ZeroFS>,
    influx: InfluxClient,
    shutdown: CancellationToken,
}

impl InfluxExporter {
    fn new(fs: Arc<ZeroFS>, influx: InfluxClient, shutdown: CancellationToken) -> Self {
        Self {
            fs,
            influx,
            shutdown,
        }
    }

    fn run_metric_exporter(self) {
        todo!()
    }
}

pub async fn start_influx_exporter(
    config: &InfluxConfig,
    fs: Arc<ZeroFS>,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    let influx = build_influx_client(config);
    let influx_exporter = InfluxExporter::new(fs, influx, shutdown);

    let mut handles = Vec::new();
    handles.push(spawn_named("influx-metric-exporter", async move {
        influx_exporter.run_metric_exporter()
    }));

    todo!()
}