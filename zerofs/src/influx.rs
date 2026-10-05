use crate::task::spawn_named;
use influxdb2::Client as InfluxClient;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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

fn build_influx_client(config: &InfluxConfig) -> InfluxClient {
    InfluxClient::new(
        config.url.clone(),
        config.org.clone(),
        config.auth_token.clone(),
    )
}
