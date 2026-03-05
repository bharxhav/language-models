use super::IoError;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Debug)]
pub enum PartialReadResult {
    Fresh,
    Missing,
    Empty,
    Data { data: Vec<u8>, new_offset: u64 },
}

/// Read up to `limit` bytes starting at `offset`.
/// Pass `u64::MAX` for `limit` to read to EOF.
/// Returns `Fresh` if offset >= file size (nothing new).
pub async fn read_partial(
    path: &Path,
    offset: u64,
    limit: u64,
) -> Result<PartialReadResult, IoError> {
    let meta = match tokio::fs::metadata(path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PartialReadResult::Missing);
        }
        Err(e) => {
            return Err(IoError::Fs {
                path: path.to_path_buf(),
                source: e,
            });
        }
    };

    let size = meta.len();
    if size == 0 {
        return Ok(PartialReadResult::Empty);
    }
    if offset >= size {
        return Ok(PartialReadResult::Fresh);
    }

    let mut file = tokio::fs::File::open(path).await.map_err(|e| IoError::Fs {
        path: path.to_path_buf(),
        source: e,
    })?;

    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| IoError::Fs {
            path: path.to_path_buf(),
            source: e,
        })?;

    let mut buf = Vec::new();
    file.take(limit)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| IoError::Fs {
            path: path.to_path_buf(),
            source: e,
        })?;

    let new_offset = offset + buf.len() as u64;

    Ok(PartialReadResult::Data {
        data: buf,
        new_offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[tokio::test]
    async fn missing_returns_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.bin");
        let result = read_partial(&path, 0, u64::MAX).await.unwrap();
        assert!(matches!(result, PartialReadResult::Missing));
    }

    #[tokio::test]
    async fn empty_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();

        let result = read_partial(&path, 0, u64::MAX).await.unwrap();
        assert!(matches!(result, PartialReadResult::Empty));
    }

    #[tokio::test]
    async fn from_zero_unlimited_returns_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full.bin");
        std::fs::write(&path, b"hello world").unwrap();

        match read_partial(&path, 0, u64::MAX).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"hello world");
                assert_eq!(new_offset, 11);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn offset_past_eof_returns_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small.bin");
        std::fs::write(&path, b"abc").unwrap();

        let result = read_partial(&path, 100, u64::MAX).await.unwrap();
        assert!(matches!(result, PartialReadResult::Fresh));
    }

    #[tokio::test]
    async fn chunked_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chunks.bin");
        std::fs::write(&path, b"aabbccdd").unwrap();

        // Read first 3 bytes.
        let off = match read_partial(&path, 0, 3).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"aab");
                assert_eq!(new_offset, 3);
                new_offset
            }
            other => panic!("expected Data, got {other:?}"),
        };

        // Read next 3 bytes.
        let off = match read_partial(&path, off, 3).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"bcc");
                assert_eq!(new_offset, 6);
                new_offset
            }
            other => panic!("expected Data, got {other:?}"),
        };

        // Read remaining — limit larger than what's left.
        match read_partial(&path, off, 100).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"dd");
                assert_eq!(new_offset, 8);
            }
            other => panic!("expected Data, got {other:?}"),
        }

        // Nothing left.
        let result = read_partial(&path, 8, u64::MAX).await.unwrap();
        assert!(matches!(result, PartialReadResult::Fresh));
    }

    #[tokio::test]
    async fn incremental_with_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.bin");

        std::fs::write(&path, b"first").unwrap();

        let offset = match read_partial(&path, 0, u64::MAX).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"first");
                assert_eq!(new_offset, 5);
                new_offset
            }
            other => panic!("expected Data, got {other:?}"),
        };

        // Append more data.
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"second").unwrap();
        drop(file);

        // Read only new bytes.
        match read_partial(&path, offset, u64::MAX).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"second");
                assert_eq!(new_offset, 11);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn offset_at_exact_eof_returns_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exact_eof.bin");
        // Write exactly 10 bytes.
        std::fs::write(&path, b"0123456789").unwrap();

        // offset == file size (10) must be Fresh, not Data.
        let result = read_partial(&path, 10, u64::MAX).await.unwrap();
        assert!(
            matches!(result, PartialReadResult::Fresh),
            "expected Fresh at offset == file size, got {result:?}"
        );
    }

    #[tokio::test]
    async fn limit_larger_than_remaining() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial_tail.bin");
        std::fs::write(&path, b"hello").unwrap();

        // Start at byte 3, ask for 100 bytes — only 2 remain ("lo").
        match read_partial(&path, 3, 100).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, b"lo", "unexpected tail bytes");
                assert_eq!(new_offset, 5, "new_offset must equal file size");
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn single_byte_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.bin");
        std::fs::write(&path, b"abc").unwrap();

        let expected: &[(&[u8], u64)] = &[(b"a", 1), (b"b", 2), (b"c", 3)];

        for (i, (expected_byte, expected_offset)) in expected.iter().enumerate() {
            match read_partial(&path, i as u64, 1).await.unwrap() {
                PartialReadResult::Data { data, new_offset } => {
                    assert_eq!(&data, expected_byte, "byte mismatch at index {i}");
                    assert_eq!(new_offset, *expected_offset, "offset mismatch at index {i}");
                }
                other => panic!("expected Data at index {i}, got {other:?}"),
            }
        }

        // After the last byte the file is exhausted.
        let result = read_partial(&path, 3, 1).await.unwrap();
        assert!(
            matches!(result, PartialReadResult::Fresh),
            "expected Fresh after last byte, got {result:?}"
        );
    }

    #[tokio::test]
    async fn binary_content_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binary.bin");

        // Construct a payload that exercises boundary bytes.
        let payload: Vec<u8> = (0u8..=255u8).collect();
        std::fs::write(&path, &payload).unwrap();

        match read_partial(&path, 0, u64::MAX).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert_eq!(data, payload, "binary content must round-trip exactly");
                assert_eq!(new_offset, 256);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_partial_permission_denied() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_perms.bin");
        std::fs::write(&path, b"secret").unwrap();

        // Remove all permissions so the file cannot be opened.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        let result = read_partial(&path, 0, u64::MAX).await;

        // Restore before any assert so the tempdir can be cleaned up.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        assert!(
            result.is_err(),
            "expected Err for permission-denied path, got {result:?}"
        );
    }

    #[tokio::test]
    async fn read_partial_directory_returns_error() {
        let dir = tempfile::tempdir().unwrap();

        // The tempdir itself is a directory — metadata will succeed but
        // opening it for reading must fail with an error.
        let result = read_partial(dir.path(), 0, u64::MAX).await;
        assert!(
            result.is_err(),
            "expected Err when path is a directory, got {result:?}"
        );
    }

    #[tokio::test]
    async fn limit_zero_returns_empty_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hello.bin");
        std::fs::write(&path, b"hello").unwrap();

        // limit=0 means take no bytes; should yield Data with empty vec.
        match read_partial(&path, 0, 0).await.unwrap() {
            PartialReadResult::Data { data, new_offset } => {
                assert!(
                    data.is_empty(),
                    "expected empty data with limit=0, got {data:?}"
                );
                assert_eq!(
                    new_offset, 0,
                    "new_offset must equal offset when no bytes read"
                );
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    proptest! {
        #[test]
        fn proptest_chunked_reassembly(
            content in proptest::collection::vec(any::<u8>(), 1..4096),
            chunk_size in 1u64..256,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let outcome: Result<Vec<u8>, proptest::test_runner::TestCaseError> =
                rt.block_on(async {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("prop.bin");
                    std::fs::write(&path, &content).unwrap();

                    let mut reassembled: Vec<u8> = Vec::with_capacity(content.len());
                    let mut offset: u64 = 0;

                    loop {
                        match read_partial(&path, offset, chunk_size).await.unwrap() {
                            PartialReadResult::Data { data, new_offset } => {
                                reassembled.extend_from_slice(&data);
                                offset = new_offset;
                            }
                            PartialReadResult::Fresh => break,
                            other => panic!("unexpected variant during reassembly: {other:?}"),
                        }
                    }

                    Ok(reassembled)
                });
            let reassembled = outcome.unwrap();
            prop_assert_eq!(reassembled, content);
        }
    }
}
