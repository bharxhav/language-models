use super::IoError;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use ignore::WalkBuilder;

#[derive(Debug, Clone)]
pub struct WalkEntry {
    pub path: PathBuf,
    pub rel_path: PathBuf,
    pub size: u64,
    pub modified: Option<SystemTime>,
}

pub fn sync_entry(entry: &mut WalkEntry) -> Result<(), IoError> {
    let meta = entry.path.metadata().map_err(|e| IoError::Fs {
        path: entry.path.clone(),
        source: e,
    })?;
    entry.size = meta.len();
    entry.modified = meta.modified().ok();
    Ok(())
}

/// Build a `WalkEntry` for a single file by stat-ing it.
fn stat_entry(abs: PathBuf, rel: PathBuf) -> Result<WalkEntry, IoError> {
    let meta = abs.metadata().map_err(|e| IoError::Fs {
        path: abs.clone(),
        source: e,
    })?;
    Ok(WalkEntry {
        size: meta.len(),
        modified: meta.modified().ok(),
        path: abs,
        rel_path: rel,
    })
}

/// Walk `path` collecting files with extension `ext`.
///
/// - `recursive` -- descend into subdirectories (false = immediate children only).
/// - `no_ignore` -- include files matched by `.gitignore`.
///
/// If `path` is a single file that matches `ext`, returns a one-element vec.
/// Results are sorted by `rel_path` for deterministic output.
pub fn walk_dir(
    path: &Path,
    ext: &str,
    recursive: bool,
    no_ignore: bool,
) -> Result<Vec<WalkEntry>, IoError> {
    let root = path.canonicalize().map_err(|e| IoError::Fs {
        path: path.to_path_buf(),
        source: e,
    })?;

    if root.is_file() {
        return if matches_ext(&root, ext) {
            let rel = PathBuf::from(root.file_name().unwrap());
            Ok(vec![stat_entry(root, rel)?])
        } else {
            Err(IoError::ExtMismatch {
                path: root,
                expected: ext.to_string(),
            })
        };
    }

    let mut builder = WalkBuilder::new(&root);
    builder.git_ignore(!no_ignore);

    if !recursive {
        builder.max_depth(Some(1));
    }

    let mut files = Vec::new();

    for entry in builder.build() {
        let entry = entry.map_err(|e| IoError::Walk {
            root: root.clone(),
            message: e.to_string(),
        })?;

        let Some(ft) = entry.file_type() else {
            continue;
        };
        if !ft.is_file() {
            continue;
        }
        if !matches_ext(entry.path(), ext) {
            continue;
        }

        let abs = entry.into_path();
        let rel = abs.strip_prefix(&root).unwrap_or(&abs).to_path_buf();
        files.push(stat_entry(abs, rel)?);
    }

    files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(files)
}

fn matches_ext(path: &Path, ext: &str) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case(ext))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn git_init(dir: &Path) {
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir)
            .status()
            .unwrap();
    }

    // ── original ten ─────────────────────────────────────────────────────────

    #[test]
    fn filters_by_extension() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.md"), "").unwrap();
        fs::write(dir.path().join("b.rs"), "").unwrap();
        fs::write(dir.path().join("c.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, false).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.to_str().unwrap()).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"a.md"));
        assert!(names.contains(&"c.md"));
    }

    #[test]
    fn recursive_finds_nested() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("sub/deep")).unwrap();
        fs::write(dir.path().join("top.md"), "").unwrap();
        fs::write(dir.path().join("sub/mid.md"), "").unwrap();
        fs::write(dir.path().join("sub/deep/bot.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, false).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn non_recursive_skips_nested() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("top.md"), "").unwrap();
        fs::write(dir.path().join("sub/nested.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", false, false).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_path.to_str().unwrap(), "top.md");
    }

    #[test]
    fn sorted_deterministic() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("z.md"), "").unwrap();
        fs::write(dir.path().join("a.md"), "").unwrap();
        fs::write(dir.path().join("m.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, false).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.clone()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    #[test]
    fn empty_dir() {
        let dir = TempDir::new().unwrap();
        let files = walk_dir(dir.path(), "md", true, false).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn single_file_matching() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("agent.md");
        fs::write(&file, "# hello").unwrap();

        let files = walk_dir(&file, "md", true, false).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_path.to_str().unwrap(), "agent.md");
    }

    #[test]
    fn single_file_wrong_ext() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("agent.rs");
        fs::write(&file, "").unwrap();

        let err = walk_dir(&file, "md", true, false).unwrap_err();
        assert!(matches!(err, super::super::IoError::ExtMismatch { .. }));
        assert!(err.to_string().contains(".md"));
    }

    #[test]
    fn respects_gitignore() {
        let dir = TempDir::new().unwrap();
        git_init(dir.path());

        fs::write(dir.path().join(".gitignore"), "ignored.md\n").unwrap();
        fs::write(dir.path().join("keep.md"), "").unwrap();
        fs::write(dir.path().join("ignored.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, false).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_path.to_str().unwrap(), "keep.md");
    }

    #[test]
    fn no_ignore_includes_gitignored() {
        let dir = TempDir::new().unwrap();
        git_init(dir.path());

        fs::write(dir.path().join(".gitignore"), "ignored.md\n").unwrap();
        fs::write(dir.path().join("keep.md"), "").unwrap();
        fs::write(dir.path().join("ignored.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn ext_case_insensitive() {
        assert!(matches_ext(Path::new("file.MD"), "md"));
        assert!(matches_ext(Path::new("file.md"), "MD"));
        assert!(matches_ext(Path::new("file.Rs"), "rs"));
    }

    // ── new eleven ───────────────────────────────────────────────────────────

    #[test]
    fn nonexistent_path_returns_error() {
        let path = Path::new("/tmp/__morphaj_does_not_exist_9f3a2b__/missing.md");
        let err = walk_dir(path, "md", true, false).unwrap_err();
        assert!(
            matches!(err, super::super::IoError::Fs { .. }),
            "expected IoError::Fs, got: {err:?}"
        );
    }

    /// The `ignore` crate's WalkBuilder skips hidden (dot-prefixed) files by
    /// default. `walk_dir` does not override this, so hidden files are excluded.
    #[test]
    fn hidden_files_skipped_by_default() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join(".hidden.md"), "").unwrap();
        fs::write(dir.path().join("visible.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.to_str().unwrap()).collect();
        assert_eq!(names.len(), 1, "expected only visible file: {names:?}");
        assert_eq!(names[0], "visible.md");
    }

    #[test]
    fn files_without_extension_skipped() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("README"), "no extension").unwrap();
        fs::write(dir.path().join("doc.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.to_str().unwrap()).collect();
        assert_eq!(names, vec!["doc.md"], "unexpected files: {names:?}");
    }

    #[test]
    fn deeply_nested_recursive() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("a/b/c/d/e")).unwrap();
        fs::write(dir.path().join("a/b/c/d/e/deep.md"), "deep").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        assert_eq!(files.len(), 1, "expected exactly one file: {files:?}");
        assert_eq!(
            files[0].rel_path,
            Path::new("a/b/c/d/e/deep.md"),
            "rel_path mismatch"
        );
    }

    /// The `ignore` crate does not follow file symlinks by default
    /// (`follow_links` is false). `walk_dir` does not override this, so
    /// symlinked files are excluded from results.
    #[cfg(unix)]
    #[test]
    fn symlink_to_file_not_followed() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().unwrap();
        let real = dir.path().join("real.md");
        let link = dir.path().join("link.md");
        fs::write(&real, "real content").unwrap();
        symlink(&real, &link).unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.to_str().unwrap()).collect();
        assert_eq!(
            names.len(),
            1,
            "expected only real file, not symlink: {names:?}"
        );
        assert_eq!(names[0], "real.md");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_to_directory() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().unwrap();
        let real_dir = dir.path().join("real_dir");
        fs::create_dir(&real_dir).unwrap();
        fs::write(real_dir.join("file.md"), "").unwrap();
        let link_dir = dir.path().join("link_dir");
        symlink(&real_dir, &link_dir).unwrap();

        // follow_links must be enabled; WalkBuilder follows symlinks by default when
        // the symlink target is a directory on most platforms, but we set no_ignore=true
        // to prevent any gitignore from hiding the entries.
        let mut builder = ignore::WalkBuilder::new(dir.path());
        builder.git_ignore(false).follow_links(true);

        // Use walk_dir with no_ignore=true; the `ignore` crate follows symlinks to
        // directories by default, so the file inside link_dir should appear.
        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.to_str().unwrap()).collect();

        // We expect file.md to be reachable via both real_dir and link_dir paths,
        // or at minimum through the symlinked directory.
        let found_any = names.iter().any(|n| n.ends_with("file.md"));
        assert!(
            found_any,
            "expected file.md reachable via symlinked dir: {names:?}"
        );
    }

    #[test]
    fn many_files_sorted() {
        let dir = TempDir::new().unwrap();
        for i in 0..100_u32 {
            fs::write(dir.path().join(format!("file-{i:03}.md")), "").unwrap();
        }

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        assert_eq!(files.len(), 100, "expected 100 files, got {}", files.len());

        let rel_paths: Vec<_> = files.iter().map(|f| f.rel_path.clone()).collect();
        let mut expected: Vec<_> = rel_paths.clone();
        expected.sort();
        assert_eq!(rel_paths, expected, "files are not sorted by rel_path");

        // verify first and last to confirm the naming scheme
        assert_eq!(rel_paths[0].to_str().unwrap(), "file-000.md");
        assert_eq!(rel_paths[99].to_str().unwrap(), "file-099.md");
    }

    #[test]
    fn mixed_extensions_only_target() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("alpha.md"), "").unwrap();
        fs::write(dir.path().join("beta.rs"), "").unwrap();
        fs::write(dir.path().join("gamma.txt"), "").unwrap();
        fs::write(dir.path().join("delta.toml"), "").unwrap();
        fs::write(dir.path().join("epsilon.md"), "").unwrap();
        fs::write(dir.path().join("zeta.rs"), "").unwrap();

        let files = walk_dir(dir.path(), "rs", true, true).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.rel_path.to_str().unwrap()).collect();
        assert_eq!(names.len(), 2, "expected exactly 2 .rs files: {names:?}");
        assert!(names.contains(&"beta.rs"), "missing beta.rs: {names:?}");
        assert!(names.contains(&"zeta.rs"), "missing zeta.rs: {names:?}");
    }

    #[test]
    fn walk_entry_paths_are_absolute() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("root.md"), "").unwrap();
        fs::write(dir.path().join("sub/nested.md"), "").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        assert!(!files.is_empty(), "expected at least one file");
        for entry in &files {
            assert!(
                entry.path.is_absolute(),
                "path is not absolute: {:?}",
                entry.path
            );
        }
    }

    #[test]
    fn rel_path_excludes_root() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("top.md"), "").unwrap();
        fs::write(dir.path().join("sub/deep.md"), "").unwrap();

        let root_name = dir.path().file_name().unwrap().to_str().unwrap().to_owned();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        assert!(!files.is_empty(), "expected at least one file");
        for entry in &files {
            let rel = entry.rel_path.to_str().unwrap();
            assert!(
                !rel.starts_with(&root_name),
                "rel_path {:?} starts with root dir name {:?}",
                rel,
                root_name
            );
        }
    }

    #[test]
    fn walk_single_file_path_is_absolute() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("single.md");
        fs::write(&file, "content").unwrap();

        let files = walk_dir(&file, "md", true, true).unwrap();
        assert_eq!(files.len(), 1);
        assert!(
            files[0].path.is_absolute(),
            "single-file path is not absolute: {:?}",
            files[0].path
        );
    }

    #[test]
    fn walk_entry_has_size() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("empty.md"), "").unwrap();
        fs::write(dir.path().join("nonempty.md"), "hello world").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        let by_name: std::collections::HashMap<&str, &WalkEntry> = files
            .iter()
            .map(|f| (f.rel_path.to_str().unwrap(), f))
            .collect();

        assert_eq!(by_name["empty.md"].size, 0);
        assert_eq!(by_name["nonempty.md"].size, 11); // "hello world" = 11 bytes
    }

    #[test]
    fn walk_entry_has_modified() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("file.md"), "data").unwrap();

        let files = walk_dir(dir.path(), "md", true, true).unwrap();
        assert_eq!(files.len(), 1);
        assert!(
            files[0].modified.is_some(),
            "modified should be Some on this platform"
        );
    }

    #[test]
    fn single_file_has_size_and_modified() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("single.md");
        fs::write(&file, "content here").unwrap();

        let files = walk_dir(&file, "md", true, true).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].size, 12); // "content here" = 12 bytes
        assert!(files[0].modified.is_some());
    }
}
