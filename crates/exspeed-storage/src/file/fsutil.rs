//! Small filesystem helpers: positional reads, directory fsync and atomic
//! sidecar writes.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

/// Read up to `buf.len()` bytes at `pos` without moving any file cursor.
/// Returns the number of bytes read; a short count means end of file.
pub fn read_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
    let mut done = 0usize;
    while done < buf.len() {
        let n = pread(file, &mut buf[done..], pos + done as u64)?;
        if n == 0 {
            break;
        }
        done += n;
    }
    Ok(done)
}

#[cfg(unix)]
fn pread(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    loop {
        match file.read_at(buf, pos) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            r => return r,
        }
    }
}

#[cfg(windows)]
fn pread(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, pos)
}

/// Fsync a directory so that creates, renames and deletes inside it are
/// durable. A no-op where directories can't be opened as files (Windows).
pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Write `data` to `path` atomically: write `path.tmp`, fsync it, rename it
/// over `path`, then fsync the parent directory. A crash leaves either the
/// old file or the new one, never a torn mix.
pub fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("atomic_write: path has no parent"))?;
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| io::Error::other("atomic_write: path has no file name"))?
        .to_os_string();
    tmp_name.push(".tmp");
    let tmp = parent.join(tmp_name);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(parent)
}

/// Remove a file, treating "already gone" as success.
pub fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_and_leaves_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.json");
        atomic_write(&p, b"one").unwrap();
        atomic_write(&p, b"two").unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"two");
        assert!(!dir.path().join("x.json.tmp").exists());
    }

    #[test]
    fn read_at_short_read_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        fs::write(&p, b"hello").unwrap();
        let f = File::open(&p).unwrap();
        let mut buf = [0u8; 10];
        assert_eq!(read_at(&f, &mut buf, 2).unwrap(), 3);
        assert_eq!(&buf[..3], b"llo");
        assert_eq!(read_at(&f, &mut buf, 99).unwrap(), 0);
    }
}
