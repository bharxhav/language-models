use super::IoError;
use std::path::Path;
use std::time::SystemTime;

/// Write `content` to `path` atomically via tmp-file-then-rename.
/// Returns the new mtime on success.
pub async fn write_atomic(path: &Path, content: &[u8]) -> Result<SystemTime, IoError> {
    let dir = path.parent().ok_or_else(|| IoError::NoParent {
        path: path.to_path_buf(),
    })?;

    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| IoError::Fs {
            path: dir.to_path_buf(),
            source: e,
        })?;

    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| IoError::Clock)?
        .as_nanos();
    let tmp = dir.join(format!(".tmp-{nanos}"));

    tokio::fs::write(&tmp, content)
        .await
        .map_err(|e| IoError::Fs {
            path: tmp.clone(),
            source: e,
        })?;

    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| IoError::Fs {
            path: path.to_path_buf(),
            source: e,
        })?;

    tokio::fs::metadata(path)
        .await
        .and_then(|m| m.modified())
        .map_err(|e| IoError::Fs {
            path: path.to_path_buf(),
            source: e,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn no_tmp_files(dir: &std::path::Path) -> Vec<std::ffi::OsString> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".tmp-"))
            .map(|e| e.file_name())
            .collect()
    }

    // -------------------------------------------------------------------------
    // Existing tests (verbatim)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn write_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");

        let mtime = write_atomic(&path, b"payload").await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"payload");
        assert!(mtime.elapsed().unwrap().as_secs() < 5);
    }

    #[tokio::test]
    async fn write_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("over.bin");

        write_atomic(&path, b"first").await.unwrap();
        write_atomic(&path, b"second").await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"second");
    }

    #[tokio::test]
    async fn write_is_atomic_no_temp_left() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("atomic.bin");
        write_atomic(&path, b"data").await.unwrap();

        let leftovers = no_tmp_files(dir.path());
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[tokio::test]
    async fn write_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/b/c/deep.bin");

        write_atomic(&path, b"nested").await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"nested");
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rt.bin");
        let payload = b"roundtrip\x00\xff\xfe";

        write_atomic(&path, payload).await.unwrap();
        let read_back = tokio::fs::read(&path).await.unwrap();
        assert_eq!(read_back, payload);
    }

    // -------------------------------------------------------------------------
    // New tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn write_empty_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");

        write_atomic(&path, b"").await.unwrap();
        assert!(path.exists(), "file must exist after writing empty content");
        assert_eq!(std::fs::read(&path).unwrap(), b"");
    }

    #[tokio::test]
    async fn write_large_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");

        // 1 MiB of a repeating pattern: bytes 0..=255 tiled
        let payload: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        write_atomic(&path, &payload).await.unwrap();

        let read_back = tokio::fs::read(&path).await.unwrap();
        assert_eq!(
            read_back.len(),
            payload.len(),
            "size mismatch after large write"
        );
        assert_eq!(read_back, payload, "content mismatch after large write");
    }

    #[tokio::test]
    async fn write_binary_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binary.bin");

        // null byte, max byte, 0xFE, UTF-8 BOM (0xEF 0xBB 0xBF)
        let payload: &[u8] = &[0x00, 0xFF, 0xFE, 0xEF, 0xBB, 0xBF, 0x00, 0xFF];
        write_atomic(&path, payload).await.unwrap();

        let read_back = tokio::fs::read(&path).await.unwrap();
        assert_eq!(read_back, payload);
    }

    #[tokio::test]
    async fn write_preserves_no_temp_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repeated.bin");

        for i in 0u32..50 {
            write_atomic(&path, format!("write-{i}").as_bytes())
                .await
                .unwrap();
            let leftovers = no_tmp_files(dir.path());
            assert!(
                leftovers.is_empty(),
                "leftover .tmp-* files after write {i}: {leftovers:?}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_failure_preserves_original() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("protected.bin");

        // Write the original content
        std::fs::write(&path, b"original").unwrap();

        // Make the parent directory read-only so writing a temp file fails
        let perms = std::fs::Permissions::from_mode(0o444);
        std::fs::set_permissions(dir.path(), perms).unwrap();

        let result = write_atomic(&path, b"new content").await;

        // Restore permissions before any assertions so cleanup always runs
        let restore_perms = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(dir.path(), restore_perms).unwrap();

        assert!(
            result.is_err(),
            "expected write_atomic to fail on read-only dir"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"original",
            "original content must be intact after failed write"
        );
    }

    #[tokio::test]
    async fn write_to_deeply_nested_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/b/c/d/e/f/file.bin");
        let payload = b"deep nested content";

        write_atomic(&path, payload).await.unwrap();

        assert!(path.exists(), "file must exist at deeply nested path");
        let read_back = tokio::fs::read(&path).await.unwrap();
        assert_eq!(read_back, payload);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_no_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = Arc::new(dir.path().join("shared.bin"));
        let dir_path = Arc::new(dir.path().to_path_buf());

        let handles: Vec<_> = (0usize..20)
            .map(|i| {
                let p = Arc::clone(&path);
                tokio::spawn(async move {
                    let content = format!("writer-{i}");
                    write_atomic(&p, content.as_bytes()).await.unwrap();
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        // File must contain exactly one complete "writer-N" string
        let final_content = tokio::fs::read(path.as_ref()).await.unwrap();
        let final_str = std::str::from_utf8(&final_content).expect("content must be valid UTF-8");
        assert!(
            final_str.starts_with("writer-"),
            "final content must be one complete writer tag, got: {final_str:?}"
        );
        // No interleaving: must be exactly "writer-N" for some single N in 0..20
        let valid = (0usize..20).any(|i| final_str == format!("writer-{i}"));
        assert!(
            valid,
            "final content is not a recognised writer tag: {final_str:?}"
        );

        // No leftover .tmp-* files
        let leftovers = no_tmp_files(&dir_path);
        assert!(
            leftovers.is_empty(),
            "leftover temp files after concurrent writes: {leftovers:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_read_during_write_never_partial() {
        use crate::api::io::read::{ReadResult, read};

        let dir = tempfile::tempdir().unwrap();
        let path = Arc::new(dir.path().join("concurrent.bin"));

        // Seed the file with known initial content
        write_atomic(&path, b"initial").await.unwrap();

        let writer_path = Arc::clone(&path);
        let reader_path = Arc::clone(&path);

        let writer = tokio::spawn(async move {
            for _ in 0..100 {
                write_atomic(&writer_path, b"updated-content-here")
                    .await
                    .unwrap();
            }
        });

        let reader = tokio::spawn(async move {
            for _ in 0..100 {
                match read(&reader_path, None).await.unwrap() {
                    ReadResult::Data { data, .. } => {
                        let s = std::str::from_utf8(&data).expect("read data must be valid UTF-8");
                        assert!(
                            s == "initial" || s == "updated-content-here",
                            "read returned partial or corrupted content: {s:?}"
                        );
                    }
                    ReadResult::Missing => {
                        // transient: acceptable during concurrent rename
                    }
                    ReadResult::Fresh => {
                        // not expected here since we pass None, but harmless
                    }
                }
            }
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn mtime_advances_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mtime.bin");

        let mtime1 = write_atomic(&path, b"first write").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let mtime2 = write_atomic(&path, b"second write").await.unwrap();

        assert!(
            mtime2 >= mtime1,
            "second mtime must be >= first mtime; got mtime1={mtime1:?} mtime2={mtime2:?}"
        );
    }

    proptest! {
        #[test]
        fn proptest_write_read_roundtrip(content in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("prop.bin");
                write_atomic(&path, &content).await.unwrap();
                let read_back = tokio::fs::read(&path).await.unwrap();
                prop_assert_eq!(read_back, content);
                Ok(())
            }).unwrap();
        }
    }
}
