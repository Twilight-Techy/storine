//! Cross-Platform OS Kernel Socket Readiness Poller (`TASK-042`, `F-NET-01`, `RULE-CPU-01`, `RULE-SAFE-01`).
//!
//! Uses `WSAPoll` (`ws2_32.dll`) on Windows and POSIX `poll` on Unix so `ReactorShard` worker
//! threads block in the OS kernel at **0.0% CPU** when connections are idle and wake in $O(k)$
//! ready-socket dispatch without busy-spinning.

use std::net::TcpStream;

#[cfg(windows)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct PollEntry {
    fd: usize,
    events: i16,
    revents: i16,
}

#[cfg(unix)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct PollEntry {
    fd: i32,
    events: i16,
    revents: i16,
}

#[cfg(windows)]
const EVENT_READ: i16 = 0x0100; // POLLRDNORM
#[cfg(windows)]
const EVENT_WRITE: i16 = 0x0010; // POLLWRNORM

#[cfg(unix)]
const EVENT_READ: i16 = 0x0001; // POLLIN
#[cfg(unix)]
const EVENT_WRITE: i16 = 0x0004; // POLLOUT

#[cfg(windows)]
#[link(name = "ws2_32")]
extern "system" {
    fn WSAPoll(fdArray: *mut PollEntry, fds: u32, timeout: i32) -> i32;
}

#[cfg(unix)]
extern "C" {
    #[link_name = "poll"]
    fn libc_poll(fds: *mut PollEntry, nfds: usize, timeout: i32) -> i32;
}

/// Reusable zero-allocation kernel socket readiness poller (`WSAPoll` / `poll`).
#[derive(Debug, Default)]
pub struct SocketPoller {
    entries: Vec<PollEntry>,
}

impl SocketPoller {
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
        }
    }

    /// Clears the poller's descriptor list for the current reactor cycle while keeping capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Registers `stream` for kernel readiness notification (`want_write` adds write-readiness).
    #[inline]
    pub fn register(&mut self, stream: &TcpStream, want_write: bool) {
        let events = if want_write {
            EVENT_READ | EVENT_WRITE
        } else {
            EVENT_READ
        };
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            self.entries.push(PollEntry {
                fd: stream.as_raw_socket() as usize,
                events,
                revents: 0,
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            self.entries.push(PollEntry {
                fd: stream.as_raw_fd(),
                events,
                revents: 0,
            });
        }
    }

    /// Waits up to `timeout_ms` in the OS kernel for at least one registered socket to become
    /// readable, writable, or closed. Returns the number of ready sockets.
    #[inline]
    pub fn wait(&mut self, timeout_ms: i32) -> usize {
        if self.entries.is_empty() {
            return 0;
        }
        #[cfg(windows)]
        {
            // SAFETY: `self.entries` is a valid contiguous buffer of `PollEntry` (`WSAPOLLFD`)
            // and `self.entries.len()` is non-zero.
            let rc = unsafe {
                WSAPoll(
                    self.entries.as_mut_ptr(),
                    self.entries.len() as u32,
                    timeout_ms,
                )
            };
            if rc > 0 {
                rc as usize
            } else {
                0
            }
        }
        #[cfg(unix)]
        {
            // SAFETY: `self.entries` is a valid contiguous buffer of `pollfd`.
            let rc = unsafe {
                libc_poll(
                    self.entries.as_mut_ptr(),
                    self.entries.len(),
                    timeout_ms,
                )
            };
            if rc > 0 {
                rc as usize
            } else {
                0
            }
        }
    }

    /// Returns `true` if socket `idx` reported readiness (readable, writable, hangup, or error).
    #[inline]
    pub fn is_ready(&self, idx: usize) -> bool {
        self.entries
            .get(idx)
            .is_some_and(|e| e.revents != 0)
    }
}

// ---------------------------------------------------------------------------------------------
// `epoll` (Linux, `TASK-090`)
// ---------------------------------------------------------------------------------------------

/// Persistent-registration readiness poller on Linux (`epoll`, level-triggered).
///
/// Unlike [`SocketPoller`], sockets are registered once and the kernel reports only the ready
/// ones, so a reactor wake-up costs `O(ready)` instead of `O(registered)`. Each socket carries a
/// caller-chosen `u64` token that is returned with its readiness.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct EpollPoller {
    epfd: i32,
    events: Vec<EpollEvent>,
    ready: Vec<u64>,
}

/// `struct epoll_event` (packed on x86-64 only, matching the kernel ABI).
#[cfg(target_os = "linux")]
#[cfg_attr(target_arch = "x86_64", repr(C, packed))]
#[cfg_attr(not(target_arch = "x86_64"), repr(C))]
#[derive(Clone, Copy, Debug, Default)]
struct EpollEvent {
    events: u32,
    data: u64,
}

#[cfg(target_os = "linux")]
const EPOLLIN: u32 = 0x001;
#[cfg(target_os = "linux")]
const EPOLLOUT: u32 = 0x004;
#[cfg(target_os = "linux")]
const EPOLLRDHUP: u32 = 0x2000;
#[cfg(target_os = "linux")]
const EPOLL_CTL_ADD: i32 = 1;
#[cfg(target_os = "linux")]
const EPOLL_CTL_DEL: i32 = 2;
#[cfg(target_os = "linux")]
const EPOLL_CTL_MOD: i32 = 3;
#[cfg(target_os = "linux")]
const EPOLL_CLOEXEC: i32 = 0o2_000_000;

#[cfg(target_os = "linux")]
extern "C" {
    fn epoll_create1(flags: i32) -> i32;
    fn epoll_ctl(epfd: i32, op: i32, fd: i32, event: *mut EpollEvent) -> i32;
    fn epoll_wait(epfd: i32, events: *mut EpollEvent, maxevents: i32, timeout: i32) -> i32;
    fn close(fd: i32) -> i32;
}

#[cfg(target_os = "linux")]
impl EpollPoller {
    /// Creates an `epoll` instance reporting up to `max_events` ready sockets per wait.
    pub fn new(max_events: usize) -> std::io::Result<Self> {
        // SAFETY: plain syscall with a valid flag; the returned descriptor is owned by `Self`.
        let epfd = unsafe { epoll_create1(EPOLL_CLOEXEC) };
        if epfd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let max = max_events.clamp(1, 4096);
        Ok(Self {
            epfd,
            events: vec![EpollEvent::default(); max],
            ready: Vec::with_capacity(max),
        })
    }

    #[inline]
    fn interest(want_write: bool) -> u32 {
        if want_write {
            EPOLLIN | EPOLLOUT | EPOLLRDHUP
        } else {
            EPOLLIN | EPOLLRDHUP
        }
    }

    fn ctl(&self, op: i32, fd: i32, token: u64, want_write: bool) -> std::io::Result<()> {
        let mut ev = EpollEvent {
            events: Self::interest(want_write),
            data: token,
        };
        // SAFETY: `ev` is a valid `epoll_event` for the duration of the call; the kernel ignores
        // it for `EPOLL_CTL_DEL`. `self.epfd` is a live epoll descriptor.
        let rc = unsafe { epoll_ctl(self.epfd, op, fd, &mut ev) };
        if rc < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Registers `stream` with `token` (write interest when `want_write`).
    pub fn add(&self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;
        self.ctl(EPOLL_CTL_ADD, stream.as_raw_fd(), token, want_write)
    }

    /// Changes the write interest of a registered `stream`.
    pub fn modify(&self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;
        self.ctl(EPOLL_CTL_MOD, stream.as_raw_fd(), token, want_write)
    }

    /// Unregisters `stream` (call before closing it).
    pub fn delete(&self, stream: &TcpStream) -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;
        self.ctl(EPOLL_CTL_DEL, stream.as_raw_fd(), 0, false)
    }

    /// Waits up to `timeout_ms` (`-1` = indefinitely) and returns the tokens of the ready
    /// sockets (readable, writable, hung up or in error). `EINTR` counts as "nothing ready".
    pub fn wait(&mut self, timeout_ms: i32) -> &[u64] {
        self.ready.clear();
        // SAFETY: `self.events` is a valid buffer of `self.events.len()` (<= 4096) entries.
        let rc = unsafe {
            epoll_wait(self.epfd, self.events.as_mut_ptr(), self.events.len() as i32, timeout_ms)
        };
        for ev in self.events.iter().take(rc.max(0) as usize) {
            // Copy out of the (possibly packed) struct before use.
            let token = ev.data;
            self.ready.push(token);
        }
        &self.ready
    }
}

#[cfg(target_os = "linux")]
impl Drop for EpollPoller {
    fn drop(&mut self) {
        // SAFETY: `self.epfd` is owned by `self` and closed exactly once.
        unsafe {
            close(self.epfd);
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod epoll_tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    #[test]
    fn test_epoll_reports_only_ready_sockets_and_tracks_interest() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..8 {
            clients.push(TcpStream::connect(addr).unwrap());
            servers.push(listener.accept().unwrap().0);
        }
        let mut ep = EpollPoller::new(64).unwrap();
        for (i, s) in servers.iter().enumerate() {
            s.set_nonblocking(true).unwrap();
            ep.add(s, i as u64, false).unwrap();
        }
        assert!(ep.wait(0).is_empty(), "nothing readable yet");
        clients[3].write_all(b"x").unwrap();
        clients[6].write_all(b"y").unwrap();
        let mut ready: Vec<u64> = ep.wait(1000).to_vec();
        ready.sort_unstable();
        assert_eq!(ready, vec![3, 6]);
        // Write interest: an idle connected socket is immediately writable.
        ep.modify(&servers[1], 1, true).unwrap();
        assert!(ep.wait(1000).contains(&1));
        ep.modify(&servers[1], 1, false).unwrap();
        ep.delete(&servers[3]).unwrap();
        let mut ready: Vec<u64> = ep.wait(100).to_vec();
        ready.sort_unstable();
        assert_eq!(ready, vec![6], "deleted socket no longer reported; level-triggered keeps 6");
    }
}
