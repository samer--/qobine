use serde::{Deserialize, Serialize};

/// Queue version pair used across Qobuz Connect messages.
///
/// Inlined from `qconnect-core` so the protocol crate stays self-contained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QueueVersion {
    pub major: u64,
    pub minor: u64,
}

impl QueueVersion {
    pub const fn new(major: u64, minor: u64) -> Self {
        Self { major, minor }
    }

    pub const fn next_minor(self) -> Self {
        Self {
            major: self.major,
            minor: self.minor.saturating_add(1),
        }
    }
}
