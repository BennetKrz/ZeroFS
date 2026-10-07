pub struct MetricsSnapshot {
    pub fs: FsCounters,
    pub used_bytes: u64,
    pub used_inodes: u64,
    pub max_bytes: u64,
    pub mem: JemallocMemStats,
    pub segments: SegmentSnapshot,
    pub dedup: DedupStats,
    pub lsm: Vec<(String, LsmValue)>,
}

impl MetricsSnapshot {
    pub fn collect(fs: Arc<ZeroFS>) -> Self {
        zerofs_files_created_total
    }
}

pub struct FsCounters {
    zerofs_files_created_total: u64,
    zerofs_files_deleted_total: u64,
    zerofs_files_renamed_total: u64,
}
