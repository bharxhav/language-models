use super::IoError;
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug)]
pub enum ReadResult {
    Fresh,
    Data {
        data: Vec<u8>,
        mtime: SystemTime,
        size: u64,
    },
    Missing,
}

/// Read raw bytes from `path`, skipping if `known_mtime` matches.
pub async fn read(path: &Path, known_mtime: Option<SystemTime>) -> Result<ReadResult, IoError> {
    let meta = match tokio::fs::metadata(path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReadResult::Missing);
        }
        Err(e) => {
            return Err(IoError::Fs {
                path: path.to_path_buf(),
                source: e,
            });
        }
    };

    let mtime = meta.modified().map_err(|e| IoError::Fs {
        path: path.to_path_buf(),
        source: e,
    })?;

    if let Some(known) = known_mtime {
        if known == mtime {
            return Ok(ReadResult::Fresh);
        }
    }

    let data = tokio::fs::read(path).await.map_err(|e| IoError::Fs {
        path: path.to_path_buf(),
        source: e,
    })?;

    let size = data.len() as u64;

    Ok(ReadResult::Data { data, mtime, size })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[tokio::test]
    async fn read_missing_returns_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.bin");
        let result = read(&path, None).await.unwrap();
        assert!(matches!(result, ReadResult::Missing));
    }

    #[tokio::test]
    async fn read_existing_returns_data_with_mtime_and_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.bin");
        std::fs::write(&path, b"hello bytes").unwrap();

        match read(&path, None).await.unwrap() {
            ReadResult::Data { data, mtime, size } => {
                assert_eq!(data, b"hello bytes");
                assert_eq!(size, 11);
                assert!(mtime.elapsed().unwrap().as_secs() < 5);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_empty_file_returns_data_with_empty_vec_and_zero_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();

        match read(&path, None).await.unwrap() {
            ReadResult::Data { data, size, .. } => {
                assert!(data.is_empty());
                assert_eq!(size, 0);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_with_matching_mtime_returns_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh.bin");
        std::fs::write(&path, b"content").unwrap();

        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        let result = read(&path, Some(mtime)).await.unwrap();
        assert!(matches!(result, ReadResult::Fresh));
    }

    #[tokio::test]
    async fn read_with_stale_mtime_returns_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale.bin");
        std::fs::write(&path, b"new content").unwrap();

        let result = read(&path, Some(std::time::UNIX_EPOCH)).await.unwrap();
        match result {
            ReadResult::Data { data, size, .. } => {
                assert_eq!(data, b"new content");
                assert_eq!(size, 11);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_binary_content_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binary.bin");
        let content: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0x01, 0x80, 0x7F, 0x00, 0xFF];
        std::fs::write(&path, &content).unwrap();

        match read(&path, None).await.unwrap() {
            ReadResult::Data { data, .. } => {
                assert_eq!(data, content);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_size_matches_actual_len() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sized.bin");
        let content = b"known content of known length";
        std::fs::write(&path, content).unwrap();

        match read(&path, None).await.unwrap() {
            ReadResult::Data { data, size, .. } => {
                assert_eq!(size, data.len() as u64);
                assert_eq!(size, content.len() as u64);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_returns_current_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mtime.bin");
        std::fs::write(&path, b"first").unwrap();

        let original_mtime = std::fs::metadata(&path).unwrap().modified().unwrap();

        // Sleep long enough for the filesystem mtime resolution (HFS+ is 1s on macOS,
        // ext4 is 1ns; use 1100ms to be safe on all platforms in CI).
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        std::fs::write(&path, b"second").unwrap();

        match read(&path, None).await.unwrap() {
            ReadResult::Data { mtime, .. } => {
                assert_ne!(
                    mtime, original_mtime,
                    "mtime should advance after overwrite"
                );
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_permission_denied() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("noperm.bin");
        std::fs::write(&path, b"secret").unwrap();

        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&path, perms).unwrap();

        let result = read(&path, None).await;

        // Restore before any assertion so the tempdir can be cleaned up even on failure.
        let mut restore = std::fs::metadata(&path).unwrap().permissions();
        restore.set_mode(0o644);
        let _ = std::fs::set_permissions(&path, restore);

        match result {
            Err(IoError::Fs { .. }) => {}
            other => panic!("expected Err(IoError::Fs), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_directory_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        // Pass the directory itself, not a file inside it.
        let result = read(dir.path(), None).await;
        match result {
            Err(IoError::Fs { .. }) => {}
            Ok(ReadResult::Missing) => panic!("directory path must not return Missing"),
            other => panic!("expected Err(IoError::Fs), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_none_mtime_always_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("always.bin");
        std::fs::write(&path, b"payload").unwrap();

        // First read with None — must return Data.
        let first = read(&path, None).await.unwrap();
        assert!(
            matches!(first, ReadResult::Data { .. }),
            "first read (None mtime) must return Data"
        );

        // Second read with None on an unchanged file — must still return Data, not Fresh.
        let second = read(&path, None).await.unwrap();
        assert!(
            matches!(second, ReadResult::Data { .. }),
            "second read (None mtime) must return Data even when file is unchanged"
        );
    }

    proptest! {
        #[test]
        fn proptest_write_read_roundtrip(content in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("roundtrip.bin");
                tokio::fs::write(&path, &content).await.unwrap();

                match read(&path, None).await.unwrap() {
                    ReadResult::Data { data, size, .. } => {
                        prop_assert_eq!(data, content.clone());
                        prop_assert_eq!(size, content.len() as u64);
                    }
                    other => panic!("expected Data, got {other:?}"),
                }

                Ok(())
            })?;
        }
    }
}
