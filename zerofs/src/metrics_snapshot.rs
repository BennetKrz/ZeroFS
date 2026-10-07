use crate::dedup::DedupStats;
use crate::fs::ZeroFS;
use crate::rpc::server::JemallocMemStats;
use slatedb_common::metrics::Metric;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

/// Point-in-time view of every metric ZeroFS exports. Prometheus, the
/// `StreamStats` RPC and the Influx writer all read from this.
#[derive(Clone)]
pub struct MetricsSnapshot {
    pub timestamp: SystemTime,
    pub fs: FsCounters,
    pub used_bytes: u64,
    pub used_inodes: u64,
    pub max_bytes: u64,
    pub mem: JemallocMemStats,
    pub segments: SegmentSnapshot,
    pub dedup: DedupStats,
    /// Metadata LSM (SlateDB) metrics; empty when no recorder is attached.
    pub lsm: Vec<Metric>,
}

impl MetricsSnapshot {
    pub fn collect(fs: &ZeroFS) -> Self {
        let (used_bytes, used_inodes) = fs.global_stats.get_totals();
        let lsm = fs
            .db
            .slatedb_metrics()
            .map(|recorder| recorder.snapshot().all().to_vec())
            .unwrap_or_default();

        Self {
            timestamp: SystemTime::now(),
            fs: FsCounters::collect(fs),
            used_bytes,
            used_inodes,
            max_bytes: fs.max_bytes,
            mem: JemallocMemStats::read(),
            segments: SegmentSnapshot::collect(fs),
            dedup: fs.dedup.stats(),
            lsm,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FsCounters {
    pub files_created: u64,
    pub files_deleted: u64,
    pub files_renamed: u64,
    pub directories_created: u64,
    pub directories_deleted: u64,
    pub directories_renamed: u64,
    pub links_created: u64,
    pub links_deleted: u64,
    pub links_renamed: u64,
    pub read_operations: u64,
    pub write_operations: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub tombstones_created: u64,
    pub tombstones_processed: u64,
    pub tombstone_cleanup_extents_deleted: u64,
    pub tombstone_cleanup_runs: u64,
    pub total_operations: u64,
}

impl FsCounters {
    fn collect(fs: &ZeroFS) -> Self {
        let s = &fs.stats;
        let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        Self {
            files_created: load(&s.files_created),
            files_deleted: load(&s.files_deleted),
            files_renamed: load(&s.files_renamed),
            directories_created: load(&s.directories_created),
            directories_deleted: load(&s.directories_deleted),
            directories_renamed: load(&s.directories_renamed),
            links_created: load(&s.links_created),
            links_deleted: load(&s.links_deleted),
            links_renamed: load(&s.links_renamed),
            read_operations: load(&s.read_operations),
            write_operations: load(&s.write_operations),
            bytes_read: load(&s.bytes_read),
            bytes_written: load(&s.bytes_written),
            tombstones_created: load(&s.tombstones_created),
            tombstones_processed: load(&s.tombstones_processed),
            tombstone_cleanup_extents_deleted: load(&s.tombstone_cleanup_extents_deleted),
            tombstone_cleanup_runs: load(&s.tombstone_cleanup_runs),
            total_operations: load(&s.total_operations),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SegmentSnapshot {
    // Footprint gauges.
    pub segment_count: u64,
    pub appended_bytes: u64,
    pub live_bytes: u64,
    pub reclaimable_bytes: u64,
    // Reclaim state gauges.
    pub awaiting_delete: u64,
    pub awaiting_delete_bytes: u64,
    pub checkpoint_pinned: bool,
    pub unflushed_bytes: u64,
    pub active_repacks: u64,
    pub active_fetches: u64,
    pub active_puts: u64,
    pub active_deletes: u64,
    pub repack_memory_reserved_bytes: u64,
    pub repack_memory_budget_bytes: u64,
    // Reclaim counters.
    pub cycles: u64,
    pub segments_deleted: u64,
    pub deleted_bytes: u64,
    pub repack_sources: u64,
    pub frames_relocated: u64,
    pub repack_jobs: u64,
    pub orphans_reclaimed: u64,
}

impl SegmentSnapshot {
    fn collect(fs: &ZeroFS) -> Self {
        let s = fs.extent_store.segment_reclaim_stats();
        let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        let footprint = s.footprint();
        Self {
            segment_count: footprint.segment_count,
            appended_bytes: footprint.appended_bytes,
            live_bytes: footprint.live_bytes,
            reclaimable_bytes: footprint.reclaimable_bytes,
            awaiting_delete: load(&s.awaiting_delete),
            awaiting_delete_bytes: load(&s.awaiting_delete_bytes),
            checkpoint_pinned: s.checkpoint_pinned.load(Ordering::Relaxed),
            unflushed_bytes: fs.extent_store.unflushed_bytes(),
            active_repacks: load(&s.active_repacks),
            active_fetches: load(&s.active_fetches),
            active_puts: load(&s.active_puts),
            active_deletes: load(&s.active_deletes),
            repack_memory_reserved_bytes: load(&s.repack_memory_reserved_bytes),
            repack_memory_budget_bytes: load(&s.repack_memory_budget_bytes),
            cycles: load(&s.cycles),
            segments_deleted: load(&s.segments_deleted),
            deleted_bytes: load(&s.deleted_bytes),
            repack_sources: load(&s.repack_sources),
            frames_relocated: load(&s.frames_relocated),
            repack_jobs: load(&s.repack_jobs),
            orphans_reclaimed: load(&s.orphans_reclaimed),
        }
    }
}
