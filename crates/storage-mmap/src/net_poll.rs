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
            .map_or(false, |e| e.revents != 0)
    }
}
