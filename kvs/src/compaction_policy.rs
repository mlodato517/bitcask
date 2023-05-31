//! Policies for choosing when to compact immutable log files.
//!
//! This module needs a lot of work and design. It'd be nice if consumers could write their own
//! implementations of [`CompactionPolicy`]. The tricky part is, currently, I haven't found the
//! right abstraction. One idea is:
//!   1. `CompactionPolicy::command_written` which tells you what the command was and what the
//!      previous value for the key was (if any)
//!   2. `CompactionPolicy::files_compacted` which maybe just says that compaction happened but has
//!      no associated data
//!
//! It's also entirely possible that there is One Correct Policy. I haven't had the time to think
//! about what kind of workloads might warrant different policies. I originally thought workloads
//! that had differing distributions of keys would require different policies but I don't really
//! know. I might just go with lots of different benchmarks if I can write a good enough benchmark
//! harness.
//!
//! This doesn't seem bulletproof so I'm going to punt on doing anything too fancy for now...

// TODO Could this be written in a way that consumers could write their own?
pub trait CompactionPolicy: private::CompactionPolicy {
    fn should_compact(&self, context: CompactionContext) -> bool;
}

impl<T: private::CompactionPolicy> CompactionPolicy for T {
    fn should_compact(&self, context: CompactionContext) -> bool {
        private::CompactionPolicy::should_compact(self, context)
    }
}

pub struct CompactionContext {
    pub(crate) open_immutable_files: usize,
    pub(crate) dead_commands: usize,
}

pub struct MaxDeadRecordPolicy {
    max_dead_records: usize,
}
impl Default for MaxDeadRecordPolicy {
    fn default() -> Self {
        Self {
            // Factors to keep in mind:
            // 1. number of records doesn't correlate to actual file size
            // 2. number of records isn't currently correct after compaction
            // 3. number of records isn't correct between active files and immutable files
            max_dead_records: 1024,
        }
    }
}

pub struct MaxFilePolicy {
    max_files: usize,
}
impl Default for MaxFilePolicy {
    fn default() -> Self {
        Self {
            // Factors to keep in mind:
            // 1. don't want too many open files probably
            // 2. don't want to have files so big that rewriting the index takes so much memory
            // 3. don't want files so small that we have to open new ones all the time
            max_files: 8,
        }
    }
}

pub struct NeverPolicy;

mod private {
    use super::{CompactionContext, MaxDeadRecordPolicy, MaxFilePolicy, NeverPolicy};

    pub trait CompactionPolicy {
        fn should_compact(&self, context: CompactionContext) -> bool;
    }

    impl CompactionPolicy for MaxFilePolicy {
        fn should_compact(&self, context: CompactionContext) -> bool {
            context.open_immutable_files > self.max_files
        }
    }

    impl CompactionPolicy for MaxDeadRecordPolicy {
        fn should_compact(&self, context: CompactionContext) -> bool {
            // TODO For now we just delete when we have "too many dead records". What might be
            // better is when "dead records take up too much space". To accurately determine
            // that we'd need to seek to this value. Not sure if it's worth it. Need daterz.
            context.dead_commands > self.max_dead_records
        }
    }

    impl CompactionPolicy for NeverPolicy {
        fn should_compact(&self, _context: CompactionContext) -> bool {
            false
        }
    }
}
