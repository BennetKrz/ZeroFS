use crate::task::spawn_named;
use influxdb2::Client as InfluxClient;
use influxdb2::models::DataPoint;
use influxdb2::models::data_point::{DataPointBuilder, DataPointError};
use slatedb_common::metrics::{Metric, MetricValue};
use tokio::sync::mpsc::error::TryRecvError;
use tokio_stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::config::InfluxConfig;
use crate::fs::ZeroFS;
use crate::fs::tracing::{FileAccessEvent, FileOperation};
use crate::metrics_snapshot::MetricsSnapshot;
use crate::object_trace::{ObjectAccessEvent, ObjectOperation};

struct InfluxExporter {
    fs: Arc<ZeroFS>,
    influx: InfluxClient,
    config: InfluxConfig,
    shutdown: CancellationToken,
}

impl InfluxExporter {
    fn new(fs: Arc<ZeroFS>, config: &InfluxConfig, shutdown: CancellationToken) -> Self {
        let influx = build_influx_client(config);
        Self {
            fs,
            influx,
            config: config.clone(),
            shutdown,
        }
    }

    async fn run_metric_exporter(self) {
        use tokio::sync::mpsc::channel;

        let bucket = self.config.bucket;

        let mut interval_send =
            time::interval(Duration::from_millis(10_000 /*self.config.interval*/));

        let mut interval_stats = time::interval(Duration::from_millis(250));

        let (tx, rx) = channel::<DataPoint>(100_000);

        tokio::select! {
            _ = interval_send.tick() => {

                let mut points = vec![];
                loop {
                    match rx.try_recv() {
                        Ok(point) => points.push(point),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                }
                
                self.influx.write(&bucket, futures::stream::iter(points)).await;
            }
            _ = interval_stats.tick() => {
                let stats = MetricsSnapshot::collect(&self.fs);
                let points = snapshot_points(&stats);
                if points.is_err() {
                    tracing::warn!("Failed converting metrics {:?}", points.err());
                } else if let Err(e) = tx.send(futures::stream::iter(points.unwrap()).collect::<DataPoint>()).await {
                    tracing::warn!("Failed to send snapshot {:?}", e);
                }
            }
            _ = self.shutdown.cancelled() => {
                // TODO: Write to flush all remaining data.
            }       
        }

        todo!()
    }
}

pub async fn start_influx_exporter(
    config: &InfluxConfig,
    fs: Arc<ZeroFS>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let influx_exporter = InfluxExporter::new(fs, config, shutdown);

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

/// Influx integer fields are signed, clamp counters that would overflow.
fn int(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Nanoseconds since the epoch, the precision `Client::write` uses.
fn timestamp_ns(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Line protocol has no NaN or infinity, so non-finite floats are left out.
fn float_field(builder: DataPointBuilder, name: &str, v: f64) -> DataPointBuilder {
    if v.is_finite() {
        builder.field(name, v)
    } else {
        builder
    }
}

fn snapshot_points(s: &MetricsSnapshot) -> Result<Vec<DataPoint>, DataPointError> {
    let ts = timestamp_ns(s.timestamp);
    let fs = &s.fs;
    let seg = &s.segments;

    let mut points = vec![
        DataPoint::builder("zerofs_fs")
            .field("files_created", int(fs.files_created))
            .field("files_deleted", int(fs.files_deleted))
            .field("files_renamed", int(fs.files_renamed))
            .field("directories_created", int(fs.directories_created))
            .field("directories_deleted", int(fs.directories_deleted))
            .field("directories_renamed", int(fs.directories_renamed))
            .field("links_created", int(fs.links_created))
            .field("links_deleted", int(fs.links_deleted))
            .field("links_renamed", int(fs.links_renamed))
            .field("read_operations", int(fs.read_operations))
            .field("write_operations", int(fs.write_operations))
            .field("bytes_read", int(fs.bytes_read))
            .field("bytes_written", int(fs.bytes_written))
            .field("tombstones_created", int(fs.tombstones_created))
            .field("tombstones_processed", int(fs.tombstones_processed))
            .field(
                "tombstone_cleanup_extents_deleted",
                int(fs.tombstone_cleanup_extents_deleted),
            )
            .field("tombstone_cleanup_runs", int(fs.tombstone_cleanup_runs))
            .field("total_operations", int(fs.total_operations))
            .field("used_bytes", int(s.used_bytes))
            .field("used_inodes", int(s.used_inodes))
            .field("max_bytes", int(s.max_bytes))
            .timestamp(ts)
            .build()?,
        DataPoint::builder("zerofs_jemalloc")
            .field("allocated_bytes", int(s.mem.allocated))
            .field("resident_bytes", int(s.mem.resident))
            .field("mapped_bytes", int(s.mem.mapped))
            .field("retained_bytes", int(s.mem.retained))
            .field("metadata_bytes", int(s.mem.metadata))
            .timestamp(ts)
            .build()?,
        DataPoint::builder("zerofs_segments")
            .field("segment_count", int(seg.segment_count))
            .field("appended_bytes", int(seg.appended_bytes))
            .field("live_bytes", int(seg.live_bytes))
            .field("reclaimable_bytes", int(seg.reclaimable_bytes))
            .field(
                "dead_ratio",
                if seg.appended_bytes > 0 {
                    seg.reclaimable_bytes as f64 / seg.appended_bytes as f64
                } else {
                    0.0
                },
            )
            .field("awaiting_delete", int(seg.awaiting_delete))
            .field("awaiting_delete_bytes", int(seg.awaiting_delete_bytes))
            .field("checkpoint_pinned", seg.checkpoint_pinned)
            .field("unflushed_bytes", int(seg.unflushed_bytes))
            .field("active_repacks", int(seg.active_repacks))
            .field("active_fetches", int(seg.active_fetches))
            .field("active_puts", int(seg.active_puts))
            .field("active_deletes", int(seg.active_deletes))
            .field(
                "repack_memory_reserved_bytes",
                int(seg.repack_memory_reserved_bytes),
            )
            .field(
                "repack_memory_budget_bytes",
                int(seg.repack_memory_budget_bytes),
            )
            .field("reclaim_cycles", int(seg.cycles))
            .field("segments_deleted", int(seg.segments_deleted))
            .field("deleted_bytes", int(seg.deleted_bytes))
            .field("repack_sources", int(seg.repack_sources))
            .field("frames_relocated", int(seg.frames_relocated))
            .field("repack_jobs", int(seg.repack_jobs))
            .field("orphans_reclaimed", int(seg.orphans_reclaimed))
            .timestamp(ts)
            .build()?,
        DataPoint::builder("zerofs_dedup")
            .field("retained_results", int(s.dedup.retained_results as u64))
            .field("inflight_ids", int(s.dedup.inflight_ids as u64))
            .field(
                "replay_pinned_results",
                int(s.dedup.replay_pinned_results as u64),
            )
            .timestamp(ts)
            .build()?,
    ];

    for metric in &s.lsm {
        points.push(lsm_point(metric, ts)?);
    }

    Ok(points)
}

/// One point per engine metric
fn lsm_point(metric: &Metric, ts: i64) -> Result<DataPoint, DataPointError> {
    let name = metric.name.strip_prefix("slatedb.").unwrap_or(&metric.name);
    let mut builder = DataPoint::builder("zerofs_lsm").tag("name", name);
    for (key, value) in &metric.labels {
        builder = builder.tag(key.as_str(), value.as_str());
    }

    builder = match &metric.value {
        MetricValue::Counter(v) => builder.field("value", int(*v)),
        MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => builder.field("value", *v),
        MetricValue::Histogram {
            count,
            sum,
            min,
            max,
            boundaries,
            bucket_counts,
        } => {
            builder = builder.field("count", int(*count));
            builder = float_field(builder, "sum", *sum);
            builder = float_field(builder, "min", *min);
            builder = float_field(builder, "max", *max);
            // bucket_counts has one more entry than boundaries: the last
            // bucket holds everything above the highest boundary.
            for (i, n) in bucket_counts.iter().enumerate() {
                let field = match boundaries.get(i) {
                    Some(le) => format!("bucket_le_{le}"),
                    None => "bucket_le_inf".to_string(),
                };
                builder = builder.field(field, int(*n));
            }
            builder
        }
    };

    builder.timestamp(ts).build()
}

/// `received_at` is taken when the event is pulled off the broadcast
/// channel: the event's own timestamp has only second precision, and points
/// sharing measurement, tags and timestamp overwrite each other in Influx.
/// The path is a field, not a tag, to keep series cardinality bounded.
fn file_access_point(
    event: &FileAccessEvent,
    received_at: SystemTime,
) -> Result<DataPoint, DataPointError> {
    let (op, builder) = match &event.operation {
        FileOperation::Read { offset, length } => (
            "read",
            DataPoint::builder("zerofs_file_access")
                .field("offset", int(*offset))
                .field("length", int(*length)),
        ),
        FileOperation::Write { offset, length } => (
            "write",
            DataPoint::builder("zerofs_file_access")
                .field("offset", int(*offset))
                .field("length", int(*length)),
        ),
        FileOperation::Trim { offset, length } => (
            "trim",
            DataPoint::builder("zerofs_file_access")
                .field("offset", int(*offset))
                .field("length", int(*length)),
        ),
        FileOperation::Fallocate {
            offset,
            length,
            mode,
        } => (
            "fallocate",
            DataPoint::builder("zerofs_file_access")
                .field("offset", int(*offset))
                .field("length", int(*length))
                .field("mode", i64::from(*mode)),
        ),
        FileOperation::Create { mode } => (
            "create",
            DataPoint::builder("zerofs_file_access").field("mode", i64::from(*mode)),
        ),
        FileOperation::Mkdir { mode } => (
            "mkdir",
            DataPoint::builder("zerofs_file_access").field("mode", i64::from(*mode)),
        ),
        FileOperation::Mknod { mode } => (
            "mknod",
            DataPoint::builder("zerofs_file_access").field("mode", i64::from(*mode)),
        ),
        FileOperation::Setattr { mode } => {
            let builder = DataPoint::builder("zerofs_file_access");
            let builder = match mode {
                Some(mode) => builder.field("mode", i64::from(*mode)),
                None => builder,
            };
            ("setattr", builder)
        }
        FileOperation::Readdir { count } => (
            "readdir",
            DataPoint::builder("zerofs_file_access").field("count", i64::from(*count)),
        ),
        FileOperation::Rename { new_path } => (
            "rename",
            DataPoint::builder("zerofs_file_access").field("new_path", new_path.as_str()),
        ),
        FileOperation::Link { new_path } => (
            "link",
            DataPoint::builder("zerofs_file_access").field("new_path", new_path.as_str()),
        ),
        FileOperation::Symlink { target } => (
            "symlink",
            DataPoint::builder("zerofs_file_access").field("target", target.as_str()),
        ),
        FileOperation::Lookup { filename } => (
            "lookup",
            DataPoint::builder("zerofs_file_access").field("filename", filename.as_str()),
        ),
        FileOperation::Remove => ("remove", DataPoint::builder("zerofs_file_access")),
        FileOperation::Fsync => ("fsync", DataPoint::builder("zerofs_file_access")),
    };

    builder
        .tag("operation", op)
        .field("path", event.path.as_str())
        .timestamp(timestamp_ns(received_at))
        .build()
}

/// See `file_access_point` for why `received_at` is passed in and why the
/// path is a field.
fn object_access_point(
    event: &ObjectAccessEvent,
    received_at: SystemTime,
) -> Result<DataPoint, DataPointError> {
    let mut builder = DataPoint::builder("zerofs_object_access");
    let op = match &event.operation {
        ObjectOperation::Get { offset, length } => {
            if let Some(offset) = offset {
                builder = builder.field("offset", int(*offset));
            }
            if let Some(length) = length {
                builder = builder.field("length", int(*length));
            }
            "get"
        }
        ObjectOperation::Head => "head",
        ObjectOperation::Put { size } => {
            builder = builder.field("size", int(*size));
            "put"
        }
        ObjectOperation::PutMultipart => "put_multipart",
        ObjectOperation::Delete => "delete",
        ObjectOperation::List => "list",
        ObjectOperation::Copy { to } => {
            builder = builder.field("target_path", to.as_str());
            "copy"
        }
        ObjectOperation::Rename { to } => {
            builder = builder.field("target_path", to.as_str());
            "rename"
        }
    };
    if let Some(us) = event.duration_us {
        builder = builder.field("duration_us", int(us));
    }

    builder
        .tag("operation", op)
        .tag("error", if event.error { "true" } else { "false" })
        .field("path", event.path.as_str())
        .timestamp(timestamp_ns(received_at))
        .build()
}
