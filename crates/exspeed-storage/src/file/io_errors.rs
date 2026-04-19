use std::io;

/// Best-effort detection of "disk is full" errors. Uses the stable
/// `io::ErrorKind::StorageFull` / `QuotaExceeded` (Rust 1.83+), with a
/// raw OS errno fallback for older libstd (should never fire on our MSRV).
///
/// Linux: ENOSPC = 28, EDQUOT = 122.
/// macOS: ENOSPC = 28, EDQUOT = 69.
pub fn is_storage_full(err: &io::Error) -> bool {
    if matches!(
        err.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    ) {
        return true;
    }

    #[cfg(target_os = "linux")]
    {
        matches!(err.raw_os_error(), Some(28) | Some(122))
    }
    #[cfg(target_os = "macos")]
    {
        matches!(err.raw_os_error(), Some(28) | Some(69))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    #[test]
    fn storage_full_kind_recognised() {
        let e = Error::new(ErrorKind::StorageFull, "no space");
        assert!(is_storage_full(&e));
    }

    #[test]
    fn quota_exceeded_kind_recognised() {
        let e = Error::new(ErrorKind::QuotaExceeded, "quota");
        assert!(is_storage_full(&e));
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn enospc_raw_errno_recognised() {
        let e = Error::from_raw_os_error(28);
        assert!(is_storage_full(&e));
    }

    #[test]
    fn unrelated_error_not_recognised() {
        let e = Error::new(ErrorKind::PermissionDenied, "nope");
        assert!(!is_storage_full(&e));
    }
}
