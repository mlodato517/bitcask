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
//
// TODO Consider a "dead record" policy. This isn't currently supported. Originally, we just
// deleted when we had "too many dead records". What might be better is when "dead records take up
// too much space".

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
    use super::{CompactionContext, MaxFilePolicy, NeverPolicy};

    pub trait CompactionPolicy {
        fn should_compact(&self, context: CompactionContext) -> bool;
    }

    impl CompactionPolicy for MaxFilePolicy {
        fn should_compact(&self, context: CompactionContext) -> bool {
            context.open_immutable_files > self.max_files
        }
    }

    impl CompactionPolicy for NeverPolicy {
        fn should_compact(&self, _context: CompactionContext) -> bool {
            false
        }
    }
}
