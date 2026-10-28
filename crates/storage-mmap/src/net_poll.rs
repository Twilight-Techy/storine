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

/// Readiness poller with persistent registration (`epoll` on Linux, AFD poll on Windows):
/// sockets are registered once with a caller token and only ready ones are reported, so a
/// reactor wake-up costs `O(ready)` (`F-NET-01`, `RULE-CPU-03`). Level-triggered semantics.
#[cfg(any(target_os = "linux", windows))]
pub trait ReadyPoller {
    /// Registers `stream` with `token` (write interest when `want_write`).
    fn add(&mut self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()>;
    /// Changes the write interest of a registered `stream`.
    fn modify(&mut self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()>;
    /// Unregisters `stream` (call before closing it).
    fn delete(&mut self, stream: &TcpStream) -> std::io::Result<()>;
    /// Waits up to `timeout_ms` (`-1` = indefinitely); returns the tokens of ready sockets.
    fn wait(&mut self, timeout_ms: i32) -> &[u64];
}

#[cfg(target_os = "linux")]
impl ReadyPoller for EpollPoller {
    fn add(&mut self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()> {
        EpollPoller::add(self, stream, token, want_write)
    }
    fn modify(&mut self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()> {
        EpollPoller::modify(self, stream, token, want_write)
    }
    fn delete(&mut self, stream: &TcpStream) -> std::io::Result<()> {
        EpollPoller::delete(self, stream)
    }
    fn wait(&mut self, timeout_ms: i32) -> &[u64] {
        EpollPoller::wait(self, timeout_ms)
    }
}

/// Stable per-socket token (the OS descriptor / socket handle).
#[inline]
pub fn socket_token(stream: &TcpStream) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        stream.as_raw_fd() as u64
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawSocket;
        stream.as_raw_socket()
    }
}

// ---------------------------------------------------------------------------------------------
// Windows: AFD poll over an I/O completion port (`TASK-102`).
//
// The technique used by `wepoll` and `mio`: every socket's base provider handle gets an
// `IOCTL_AFD_POLL` request submitted to the Ancillary Function Driver through one `\Device\Afd`
// handle associated with a completion port. A request completes when the socket becomes
// readable / writable / closed, and `GetQueuedCompletionStatusEx` returns only those, so a wake-up
// is `O(ready)` like `epoll` (`WSAPoll` is `O(registered)` and has known bugs). Requests are
// re-armed before each wait, which gives level-triggered semantics.
// ---------------------------------------------------------------------------------------------

#[cfg(windows)]
mod afd {
    #![allow(non_snake_case, clippy::upper_case_acronyms)]
    use core::ffi::c_void;

    pub type HANDLE = isize;
    pub type NTSTATUS = i32;

    pub const INVALID_HANDLE_VALUE: HANDLE = -1;
    pub const STATUS_PENDING: NTSTATUS = 0x0000_0103;
    pub const STATUS_CANCELLED: NTSTATUS = 0xC000_0120_u32 as i32;
    pub const IOCTL_AFD_POLL: u32 = 0x0001_2024;
    pub const SIO_BASE_HANDLE: u32 = 0x4800_0022;
    pub const SIO_BSP_HANDLE_POLL: u32 = 0x4800_001D;
    pub const SYNCHRONIZE: u32 = 0x0010_0000;
    pub const FILE_SHARE_READ: u32 = 1;
    pub const FILE_SHARE_WRITE: u32 = 2;
    pub const FILE_OPEN: u32 = 1;
    pub const OBJ_CASE_INSENSITIVE: u32 = 0x40;
    pub const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x2;
    pub const INFINITE: u32 = 0xFFFF_FFFF;

    pub const AFD_POLL_RECEIVE: u32 = 0x0001;
    pub const AFD_POLL_RECEIVE_EXPEDITED: u32 = 0x0002;
    pub const AFD_POLL_SEND: u32 = 0x0004;
    pub const AFD_POLL_DISCONNECT: u32 = 0x0008;
    pub const AFD_POLL_ABORT: u32 = 0x0010;
    pub const AFD_POLL_LOCAL_CLOSE: u32 = 0x0020;
    pub const AFD_POLL_ACCEPT: u32 = 0x0080;
    pub const AFD_POLL_CONNECT_FAIL: u32 = 0x0100;

    #[repr(C)]
    pub struct UnicodeString {
        pub length: u16,
        pub maximum_length: u16,
        pub buffer: *mut u16,
    }

    #[repr(C)]
    pub struct ObjectAttributes {
        pub length: u32,
        pub root_directory: HANDLE,
        pub object_name: *mut UnicodeString,
        pub attributes: u32,
        pub security_descriptor: *mut c_void,
        pub security_quality_of_service: *mut c_void,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    pub struct IoStatusBlock {
        /// `NTSTATUS` (or a pointer) in a pointer-sized union.
        pub status: isize,
        pub information: usize,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    pub struct AfdPollHandleInfo {
        pub handle: HANDLE,
        pub events: u32,
        pub status: NTSTATUS,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    pub struct AfdPollInfo {
        pub timeout: i64,
        pub number_of_handles: u32,
        pub exclusive: u32,
        pub handles: [AfdPollHandleInfo; 1],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct OverlappedEntry {
        pub completion_key: usize,
        pub overlapped: *mut c_void,
        pub internal: usize,
        pub bytes_transferred: u32,
    }

    #[link(name = "kernel32")]
    extern "system" {
        pub fn CreateIoCompletionPort(file: HANDLE, existing: HANDLE, key: usize, threads: u32) -> HANDLE;
        pub fn GetQueuedCompletionStatusEx(
            port: HANDLE,
            entries: *mut OverlappedEntry,
            count: u32,
            removed: *mut u32,
            millis: u32,
            alertable: i32,
        ) -> i32;
        pub fn CloseHandle(handle: HANDLE) -> i32;
        pub fn SetFileCompletionNotificationModes(handle: HANDLE, flags: u8) -> i32;
    }

    #[link(name = "ntdll")]
    extern "system" {
        pub fn NtCreateFile(
            handle: *mut HANDLE,
            access: u32,
            attributes: *mut ObjectAttributes,
            iosb: *mut IoStatusBlock,
            allocation_size: *mut i64,
            file_attributes: u32,
            share: u32,
            disposition: u32,
            options: u32,
            ea: *mut c_void,
            ea_len: u32,
        ) -> NTSTATUS;
        pub fn NtDeviceIoControlFile(
            handle: HANDLE,
            event: HANDLE,
            apc: *mut c_void,
            apc_context: *mut c_void,
            iosb: *mut IoStatusBlock,
            code: u32,
            input: *mut c_void,
            input_len: u32,
            output: *mut c_void,
            output_len: u32,
        ) -> NTSTATUS;
        pub fn NtCancelIoFileEx(handle: HANDLE, request: *mut IoStatusBlock, iosb: *mut IoStatusBlock) -> NTSTATUS;
        pub fn RtlNtStatusToDosError(status: NTSTATUS) -> u32;
    }

    #[link(name = "ws2_32")]
    extern "system" {
        pub fn WSAIoctl(
            s: usize,
            code: u32,
            in_buf: *mut c_void,
            in_len: u32,
            out_buf: *mut c_void,
            out_len: u32,
            returned: *mut u32,
            overlapped: *mut c_void,
            routine: *mut c_void,
        ) -> i32;
    }
}

/// One socket's AFD poll request. Boxed: the kernel writes `iosb` and `info` until the request
/// completes, so the allocation must not move or be freed while `pending`.
#[cfg(windows)]
struct AfdOp {
    iosb: afd::IoStatusBlock,
    info: afd::AfdPollInfo,
    token: u64,
    base: afd::HANDLE,
    want_write: bool,
    pending: bool,
    deleted: bool,
}

/// Windows readiness poller: AFD poll requests completed through an I/O completion port.
#[cfg(windows)]
pub struct AfdPoller {
    port: afd::HANDLE,
    afd: afd::HANDLE,
    ops: std::collections::HashMap<u64, Box<AfdOp>>,
    /// Address of each live op's allocation -> its token (completion packets carry the address).
    by_addr: std::collections::HashMap<usize, u64>,
    /// Unregistered ops whose request is still in flight (freed when its completion arrives).
    zombies: std::collections::HashMap<usize, Box<AfdOp>>,
    entries: Vec<afd::OverlappedEntry>,
    ready: Vec<u64>,
}

#[cfg(windows)]
impl core::fmt::Debug for AfdPoller {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AfdPoller").field("sockets", &self.ops.len()).finish_non_exhaustive()
    }
}

#[cfg(windows)]
fn nt_error(status: afd::NTSTATUS) -> std::io::Error {
    // SAFETY: pure conversion function without pointers.
    let code = unsafe { afd::RtlNtStatusToDosError(status) };
    std::io::Error::from_raw_os_error(code as i32)
}

#[cfg(windows)]
impl AfdPoller {
    /// Creates the completion port and the `\Device\Afd` handle; reports up to `max_events`
    /// completions per wait.
    pub fn new(max_events: usize) -> std::io::Result<Self> {
        // SAFETY: creating a fresh completion port has no pointer arguments.
        let port = unsafe { afd::CreateIoCompletionPort(afd::INVALID_HANDLE_VALUE, 0, 0, 0) };
        if port == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut name: Vec<u16> = r"\Device\Afd\Storine".encode_utf16().collect();
        let mut uname = afd::UnicodeString {
            length: (name.len() * 2) as u16,
            maximum_length: (name.len() * 2) as u16,
            buffer: name.as_mut_ptr(),
        };
        let mut attrs = afd::ObjectAttributes {
            length: core::mem::size_of::<afd::ObjectAttributes>() as u32,
            root_directory: 0,
            object_name: &mut uname,
            attributes: afd::OBJ_CASE_INSENSITIVE,
            security_descriptor: core::ptr::null_mut(),
            security_quality_of_service: core::ptr::null_mut(),
        };
        let mut iosb = afd::IoStatusBlock::default();
        let mut handle: afd::HANDLE = 0;
        // SAFETY: every pointer refers to a live local for the duration of the call.
        let status = unsafe {
            afd::NtCreateFile(
                &mut handle,
                afd::SYNCHRONIZE,
                &mut attrs,
                &mut iosb,
                core::ptr::null_mut(),
                0,
                afd::FILE_SHARE_READ | afd::FILE_SHARE_WRITE,
                afd::FILE_OPEN,
                0,
                core::ptr::null_mut(),
                0,
            )
        };
        if status != 0 {
            // SAFETY: `port` is owned here and closed once.
            unsafe { afd::CloseHandle(port) };
            return Err(nt_error(status));
        }
        // SAFETY: both handles are live; associating and setting notification modes is safe.
        let ok = unsafe {
            afd::CreateIoCompletionPort(handle, port, 0, 0) != 0
                && afd::SetFileCompletionNotificationModes(handle, afd::FILE_SKIP_SET_EVENT_ON_HANDLE) != 0
        };
        if !ok {
            let err = std::io::Error::last_os_error();
            // SAFETY: both handles are owned here and closed once.
            unsafe {
                afd::CloseHandle(handle);
                afd::CloseHandle(port);
            }
            return Err(err);
        }
        let max = max_events.clamp(1, 4096);
        Ok(Self {
            port,
            afd: handle,
            ops: std::collections::HashMap::new(),
            by_addr: std::collections::HashMap::new(),
            zombies: std::collections::HashMap::new(),
            entries: vec![
                afd::OverlappedEntry {
                    completion_key: 0,
                    overlapped: core::ptr::null_mut(),
                    internal: 0,
                    bytes_transferred: 0
                };
                max
            ],
            ready: Vec::with_capacity(max),
        })
    }

    /// The base provider socket handle (bypasses layered service providers, as AFD requires).
    fn base_handle(stream: &TcpStream) -> std::io::Result<afd::HANDLE> {
        use std::os::windows::io::AsRawSocket;
        let sock = stream.as_raw_socket() as usize;
        for code in [afd::SIO_BSP_HANDLE_POLL, afd::SIO_BASE_HANDLE] {
            let mut out: usize = 0;
            let mut returned = 0u32;
            // SAFETY: `out` and `returned` are live locals of the declared sizes; no overlapped.
            let rc = unsafe {
                afd::WSAIoctl(
                    sock,
                    code,
                    core::ptr::null_mut(),
                    0,
                    (&mut out as *mut usize).cast(),
                    core::mem::size_of::<usize>() as u32,
                    &mut returned,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                )
            };
            if rc == 0 && out != 0 && out != usize::MAX {
                return Ok(out as afd::HANDLE);
            }
        }
        Err(std::io::Error::last_os_error())
    }

    #[inline]
    fn event_mask(want_write: bool) -> u32 {
        let base = afd::AFD_POLL_RECEIVE
            | afd::AFD_POLL_RECEIVE_EXPEDITED
            | afd::AFD_POLL_DISCONNECT
            | afd::AFD_POLL_ABORT
            | afd::AFD_POLL_LOCAL_CLOSE
            | afd::AFD_POLL_ACCEPT
            | afd::AFD_POLL_CONNECT_FAIL;
        if want_write {
            base | afd::AFD_POLL_SEND
        } else {
            base
        }
    }

    /// Submits the poll request of `op` (no-op if one is already pending).
    fn arm(afd_handle: afd::HANDLE, op: &mut AfdOp) -> std::io::Result<()> {
        if op.pending || op.deleted {
            return Ok(());
        }
        op.info = afd::AfdPollInfo {
            timeout: i64::MAX,
            number_of_handles: 1,
            exclusive: 0,
            handles: [afd::AfdPollHandleInfo {
                handle: op.base,
                events: Self::event_mask(op.want_write),
                status: 0,
            }],
        };
        op.iosb = afd::IoStatusBlock { status: afd::STATUS_PENDING as isize, information: 0 };
        let ctx: *mut AfdOp = op;
        let len = core::mem::size_of::<afd::AfdPollInfo>() as u32;
        // SAFETY: `op` is boxed (stable address) and outlives the request: it is only freed
        // after its completion is dequeued (`zombies`), and Rust code never touches `iosb` /
        // `info` while the request is pending. Both pointers derive from `ctx`; `info` is the
        // input and the output buffer.
        let status = unsafe {
            let iosb = core::ptr::addr_of_mut!((*ctx).iosb);
            let info = core::ptr::addr_of_mut!((*ctx).info);
            afd::NtDeviceIoControlFile(
                afd_handle,
                0,
                core::ptr::null_mut(),
                ctx.cast(),
                iosb,
                afd::IOCTL_AFD_POLL,
                info.cast(),
                len,
                info.cast(),
                len,
            )
        };
        if status == afd::STATUS_PENDING || status == 0 {
            // Completed or pending: either way a completion packet will be queued.
            op.pending = true;
            Ok(())
        } else {
            Err(nt_error(status))
        }
    }

    /// Cancels the pending request of `op` (its completion arrives with `STATUS_CANCELLED`).
    fn cancel(afd_handle: afd::HANDLE, op: &mut AfdOp) {
        if !op.pending {
            return;
        }
        let mut cancel_iosb = afd::IoStatusBlock::default();
        // SAFETY: `op.iosb` identifies a request issued on `afd_handle` that is still alive.
        unsafe {
            afd::NtCancelIoFileEx(afd_handle, &mut op.iosb, &mut cancel_iosb);
        }
    }
}

/// Address of a boxed op (the key completion packets are matched by).
#[cfg(windows)]
#[inline]
fn op_addr(op: &AfdOp) -> usize {
    op as *const AfdOp as usize
}

#[cfg(windows)]
impl ReadyPoller for AfdPoller {
    fn add(&mut self, stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()> {
        let base = Self::base_handle(stream)?;
        let mut op = Box::new(AfdOp {
            iosb: afd::IoStatusBlock::default(),
            info: afd::AfdPollInfo::default(),
            token,
            base,
            want_write,
            pending: false,
            deleted: false,
        });
        Self::arm(self.afd, &mut op)?;
        let addr = op_addr(&op);
        if let Some(mut old) = self.ops.insert(token, op) {
            // Token reused before the old socket was deleted: retire the old request.
            self.by_addr.remove(&op_addr(&old));
            old.deleted = true;
            Self::cancel(self.afd, &mut old);
            if old.pending {
                self.zombies.insert(op_addr(&old), old);
            }
        }
        self.by_addr.insert(addr, token);
        Ok(())
    }

    fn modify(&mut self, _stream: &TcpStream, token: u64, want_write: bool) -> std::io::Result<()> {
        let afd_handle = self.afd;
        let Some(op) = self.ops.get_mut(&token) else {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        };
        if op.want_write != want_write {
            op.want_write = want_write;
            // The cancelled request completes with `STATUS_CANCELLED`; it is re-armed with the
            // new interest before the next wait.
            Self::cancel(afd_handle, op);
        }
        Ok(())
    }

    fn delete(&mut self, stream: &TcpStream) -> std::io::Result<()> {
        let token = socket_token(stream);
        let afd_handle = self.afd;
        if let Some(mut op) = self.ops.remove(&token) {
            self.by_addr.remove(&op_addr(&op));
            op.deleted = true;
            Self::cancel(afd_handle, &mut op);
            if op.pending {
                self.zombies.insert(op_addr(&op), op);
            }
        }
        Ok(())
    }

    fn wait(&mut self, timeout_ms: i32) -> &[u64] {
        self.ready.clear();
        let afd_handle = self.afd;
        for op in self.ops.values_mut() {
            // A socket that cannot be polled any more (closed under us) simply stays silent.
            let _ = Self::arm(afd_handle, op);
        }
        let mut removed = 0u32;
        let millis = if timeout_ms < 0 { afd::INFINITE } else { timeout_ms as u32 };
        // SAFETY: `entries` is a valid buffer of `entries.len()` (<= 4096) elements.
        let ok = unsafe {
            afd::GetQueuedCompletionStatusEx(
                self.port,
                self.entries.as_mut_ptr(),
                self.entries.len() as u32,
                &mut removed,
                millis,
                0,
            )
        };
        if ok == 0 {
            return &self.ready; // timeout (or error): nothing ready
        }
        for i in 0..(removed as usize).min(self.entries.len()) {
            let ptr = self.entries[i].overlapped as usize;
            if ptr == 0 {
                continue;
            }
            if let Some(mut zombie) = self.zombies.remove(&ptr) {
                zombie.pending = false;
                continue; // freed here, after the kernel is done with it
            }
            let Some(op) = self.by_addr.get(&ptr).and_then(|t| self.ops.get_mut(t)) else {
                continue;
            };
            op.pending = false;
            let status = op.iosb.status as i32;
            if status == afd::STATUS_CANCELLED {
                continue; // interest change: re-armed before the next wait
            }
            let events = if op.info.number_of_handles > 0 { op.info.handles[0].events } else { 0 };
            if status < 0 || events & Self::event_mask(op.want_write) != 0 {
                self.ready.push(op.token);
            }
        }
        &self.ready
    }
}

#[cfg(windows)]
impl Drop for AfdPoller {
    fn drop(&mut self) {
        let afd_handle = self.afd;
        for op in self.ops.values_mut() {
            Self::cancel(afd_handle, op);
        }
        // Drain completions of every in-flight request before freeing their buffers.
        let mut in_flight = self.ops.values().filter(|o| o.pending).count() + self.zombies.len();
        let mut rounds = 0;
        while in_flight > 0 && rounds < 64 {
            rounds += 1;
            let mut removed = 0u32;
            // SAFETY: as in `wait`.
            let ok = unsafe {
                afd::GetQueuedCompletionStatusEx(self.port, self.entries.as_mut_ptr(), self.entries.len() as u32, &mut removed, 100, 0)
            };
            if ok == 0 {
                break;
            }
            in_flight = in_flight.saturating_sub(removed as usize);
        }
        // SAFETY: both handles are owned by `self` and closed exactly once. Closing the AFD
        // handle cancels any request that is still outstanding.
        unsafe {
            afd::CloseHandle(self.afd);
            afd::CloseHandle(self.port);
        }
        if in_flight > 0 {
            // Requests that never reported back: leak their buffers rather than risk a
            // use-after-free by the kernel.
            for (_, op) in self.ops.drain() {
                if op.pending {
                    core::mem::forget(op);
                }
            }
            for (_, op) in self.zombies.drain() {
                core::mem::forget(op);
            }
        }
    }
}

#[cfg(all(test, windows))]
mod afd_tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    #[test]
    fn test_afd_poll_reports_only_ready_sockets_and_tracks_interest() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..8 {
            clients.push(TcpStream::connect(addr).unwrap());
            servers.push(listener.accept().unwrap().0);
        }
        let mut p = AfdPoller::new(64).unwrap();
        for (i, s) in servers.iter().enumerate() {
            s.set_nonblocking(true).unwrap();
            p.add(s, i as u64, false).unwrap();
        }
        assert!(p.wait(0).is_empty(), "nothing readable yet");
        clients[3].write_all(b"x").unwrap();
        clients[6].write_all(b"y").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut ready: Vec<u64> = p.wait(1000).to_vec();
        ready.sort_unstable();
        assert_eq!(ready, vec![3, 6]);
        // Level-triggered: still readable, reported again.
        let mut again: Vec<u64> = p.wait(1000).to_vec();
        again.sort_unstable();
        assert_eq!(again, vec![3, 6]);
        p.modify(&servers[1], 1, true).unwrap();
        let w: Vec<u64> = (0..3).flat_map(|_| p.wait(200).to_vec()).collect();
        assert!(w.contains(&1), "writable socket reported after enabling write interest");
        p.modify(&servers[1], 1, false).unwrap();
        p.delete(&servers[3]).unwrap();
        let after: Vec<u64> = (0..3).flat_map(|_| p.wait(100).to_vec()).collect();
        assert!(!after.contains(&3), "deleted socket no longer reported");
        assert!(after.contains(&6), "level-triggered keeps 6");
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
