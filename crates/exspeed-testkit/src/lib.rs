//! Helpers shared by integration tests across the workspace.

use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::Mutex;

static HANDED_OUT: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// A listener bound to `127.0.0.1:0`. Pass it to an in-process server
/// (`ServerArgs::tcp_listener` / `api_listener`): the port is the
/// listener's own, so nothing else can take it in between.
pub fn bind_local() -> TcpListener {
    TcpListener::bind(("127.0.0.1", 0)).expect("bind 127.0.0.1:0")
}

/// Return a TCP port on 127.0.0.1 that is currently free.
///
/// Unlike `portpicker`, this only needs IPv4 (it works in IPv6-less
/// containers) and never hands out the same port twice within one test
/// process, so parallel tests in a single binary don't collide. There is
/// still a window between this call and the server binding the port, so
/// use it only where the port must be known before the server exists (a
/// child process, a restart on the same port, a cluster address that is
/// advertised up front); otherwise use [`bind_local`].
pub fn pick_unused_port() -> Option<u16> {
    let mut guard = HANDED_OUT.lock().unwrap_or_else(|e| e.into_inner());
    let used = guard.get_or_insert_with(HashSet::new);
    for _ in 0..64 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).ok()?;
        let port = listener.local_addr().ok()?.port();
        if used.insert(port) {
            return Some(port);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_are_unique() {
        let a = pick_unused_port().unwrap();
        let b = pick_unused_port().unwrap();
        assert_ne!(a, b);
    }
}
