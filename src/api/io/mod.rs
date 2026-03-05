pub mod read;
pub mod read_partial;
pub mod walk;
pub mod write;

use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IoError {
    /// Real filesystem I/O failure (read, write, open, seek, rename, metadata …).
    #[error("{path}: {source}")]
    Fs {
        path: PathBuf,
        source: std::io::Error,
    },

    /// Path has no parent directory (e.g. bare filename passed to write_atomic).
    #[error("{path}: path has no parent directory")]
    NoParent { path: PathBuf },

    /// System clock is before the UNIX epoch — cannot generate temp-file names.
    #[error("system clock is before the UNIX epoch")]
    Clock,

    /// File extension doesn't match the expected value.
    #[error("{path}: expected .{expected} file")]
    ExtMismatch { path: PathBuf, expected: String },

    /// Directory walk/traversal error (from the `ignore` crate).
    #[error("walk {root}: {message}")]
    Walk { root: PathBuf, message: String },

    /// stdin/stdout/stderr I/O failure (not tied to a file path).
    #[error("stdio: {source}")]
    Stdio { source: std::io::Error },
}
