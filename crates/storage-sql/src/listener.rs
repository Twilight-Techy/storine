//! The accept loop of all three protocols (`TASK-152`): how many connections, how long a client may
//! take to log in, and a stack that holds what the parser lets through.

use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

/// A client has this long from connecting to finishing its login (`set_login_deadline`).
static LOGIN_DEADLINE_MS: AtomicU64 = AtomicU64::new(30_000);
/// Expressions can be `MAX_HEIGHT` tall (`parser.rs`) and later stages walk them recursively; the
/// default 2 MiB does not hold that. Untouched pages of a stack cost nothing.
const CONNECTION_STACK: usize = 16 << 20;

static MAX_CONNECTIONS: AtomicUsize = AtomicUsize::new(1024);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Connections beyond this (across all protocols) are closed at once.
pub fn set_max_connections(n: usize) {
    MAX_CONNECTIONS.store(n.max(1), Ordering::Relaxed);
}

/// How long a client has from connecting to finishing its login.
pub fn set_login_deadline(deadline: Duration) {
    LOGIN_DEADLINE_MS.store(deadline.as_millis() as u64, Ordering::Relaxed);
}

/// A connection's login in progress. Dropping it, or `done`, ends the deadline.
#[derive(Debug)]
pub struct Login(Arc<AtomicBool>);

impl Login {
    pub fn done(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Drop for Login {
    fn drop(&mut self) {
        self.done();
    }
}

type Pending = Vec<(Instant, TcpStream, Arc<AtomicBool>)>;

/// One thread enforces every deadline: closing the socket ends whatever read the connection is in.
fn watch_login(stream: &TcpStream) -> io::Result<Login> {
    static PENDING: OnceLock<Arc<Mutex<Pending>>> = OnceLock::new();
    let pending = PENDING.get_or_init(|| {
        let pending: Arc<Mutex<Pending>> = Arc::default();
        let list = Arc::clone(&pending);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(250));
            let now = Instant::now();
            list.lock().unwrap_or_else(std::sync::PoisonError::into_inner).retain(|(deadline, socket, done)| {
                if done.load(Ordering::Relaxed) {
                    return false;
                }
                if now < *deadline {
                    return true;
                }
                let _ = socket.shutdown(Shutdown::Both);
                false
            });
        });
        pending
    });
    let done = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_millis(LOGIN_DEADLINE_MS.load(Ordering::Relaxed));
    let entry = (deadline, stream.try_clone()?, Arc::clone(&done));
    pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(entry);
    Ok(Login(done))
}

/// Gives back a connection's place however its thread ends.
struct Place;

impl Drop for Place {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::Relaxed);
    }
}

fn refused(what: &str) {
    // At most one line a minute: this is what a flood looks like.
    static LAST: AtomicU64 = AtomicU64::new(0);
    static START: OnceLock<Instant> = OnceLock::new();
    let now = START.get_or_init(Instant::now).elapsed().as_secs() + 60;
    let last = LAST.load(Ordering::Relaxed);
    if now >= last + 60 && LAST.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
        eprintln!("storine-sql: refusing connections: {what}");
    }
}

/// Accepts on `listener` forever, one thread per connection; `name` labels what it logs.
pub fn serve<F>(name: &'static str, listener: TcpListener, handle: F)
where
    F: Fn(TcpStream, Login) -> io::Result<()> + Send + Sync + 'static,
{
    let handle = Arc::new(handle);
    for conn in listener.incoming() {
        let stream = match conn {
            Ok(s) => s,
            // Out of descriptors, say: the queue would only return the same error at once.
            Err(e) => {
                refused(&e.to_string());
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        let limit = MAX_CONNECTIONS.load(Ordering::Relaxed);
        if ACTIVE.fetch_add(1, Ordering::Relaxed) >= limit {
            ACTIVE.fetch_sub(1, Ordering::Relaxed);
            refused(&format!("{limit} connections are open (--max-connections)"));
            continue;
        }
        let place = Place;
        let Ok(login) = watch_login(&stream) else {
            continue;
        };
        let handle = Arc::clone(&handle);
        let spawned = thread::Builder::new().stack_size(CONNECTION_STACK).spawn(move || {
            let _place = place;
            let _ = stream.set_nodelay(true);
            if let Err(e) = handle(stream, login) {
                if !matches!(e.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset) {
                    eprintln!("storine-sql: {name} connection ended: {e}");
                }
            }
        });
        if let Err(e) = spawned {
            refused(&e.to_string());
        }
    }
}
