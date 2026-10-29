//! Storine SQL gateway: lets applications and ORMs written for PostgreSQL or MySQL use Storine
//! unchanged, by speaking their wire protocols and SQL dialects over the document API.

pub mod aes;
pub mod ast;
pub mod catalog;
pub mod copy;
pub mod datum;
pub mod embedded;
pub mod error;
pub mod exec;
pub mod fmt;
pub mod hash;
pub mod kerberos;
pub mod lexer;
pub mod mysql;
pub mod oidc;
pub mod parser;
pub mod pg;
pub mod rsa;
pub mod store;
pub mod tds;
pub mod tls;
pub mod vector;

pub use error::{SqlError, SqlResult};
pub use lexer::Dialect;

/// Version reported to PostgreSQL clients (`server_version`).
pub const PG_VERSION: &str = "16.0";
pub const PG_VERSION_NUM: &str = "160000";
pub const PG_VERSION_STRING: &str =
    "PostgreSQL 16.0 (Storine SQL gateway) on x86_64, compiled by rustc, 64-bit";
/// Version reported to MySQL clients.
/// `@@VERSION` on the SQL Server endpoint.
pub const MSSQL_VERSION_STRING: &str =
    "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (X64) Storine SQL gateway (compatible)";
pub const MYSQL_VERSION: &str = "8.4.3-storine";

/// A random id of this gateway process (row-lock ownership).
pub fn instance_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| hash::hex(&hash::random_bytes(8)))
}

/// Whether statements and errors are logged to stderr (`STORINE_SQL_LOG=1`).
pub fn log_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("STORINE_SQL_LOG").is_ok_and(|v| v != "0"))
}

/// Shared pieces of the in-crate fuzz sweeps (`TASK-135`) over the private decoders.
#[cfg(test)]
pub(crate) mod fuzz_support {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    pub(crate) use storage_types::testing::SweepRng;

    /// The system allocator, noting the largest request made while a thread measures.
    struct Tracking;

    thread_local! {
        static MEASURING: Cell<bool> = const { Cell::new(false) };
        static LARGEST: Cell<usize> = const { Cell::new(0) };
    }

    fn note(size: usize) {
        if MEASURING.try_with(Cell::get).unwrap_or(false) {
            let _ = LARGEST.try_with(|l| l.set(l.get().max(size)));
        }
    }

    // SAFETY: every call is forwarded unchanged to `System`; `note` only reads and writes
    // const-initialized thread-locals, which never allocate.
    unsafe impl GlobalAlloc for Tracking {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: `ptr` came from `System` with this layout (every allocation does).
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            note(new_size);
            // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: Tracking = Tracking;

    /// Largest single allocation a decoder may make for hostile input of a few KiB.
    pub(crate) const ALLOC_LIMIT: usize = 16 << 20;

    /// Runs `f`, failing if it allocated more than `ALLOC_LIMIT` bytes at once (a length
    /// field trusted before the bytes arrived).
    pub(crate) fn bounded<T>(what: &dyn std::fmt::Debug, f: impl FnOnce() -> T) -> T {
        LARGEST.with(|l| l.set(0));
        MEASURING.with(|m| m.set(true));
        let out = f();
        MEASURING.with(|m| m.set(false));
        let largest = LARGEST.with(Cell::get);
        assert!(
            largest <= ALLOC_LIMIT,
            "allocated {largest} bytes at once for {what:?}"
        );
        out
    }

    /// `base` iterations scaled by `STORINE_FUZZ_ITERS` (a factor such as `0.1` or `100`).
    pub(crate) fn iters(base: usize) -> usize {
        let scale = std::env::var("STORINE_FUZZ_ITERS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|s| s.is_finite() && *s > 0.0)
            .unwrap_or(1.0);
        ((base as f64 * scale) as usize).max(1)
    }

    /// The generator of one sweep: fixed, or varied by `STORINE_FUZZ_SEED` to explore further.
    pub(crate) fn rng(tag: u64) -> SweepRng {
        let seed = std::env::var("STORINE_FUZZ_SEED")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        SweepRng::new(tag ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15))
    }

    /// `len` random bytes.
    pub(crate) fn junk(rng: &mut SweepRng, len: usize) -> Vec<u8> {
        (0..len).map(|_| rng.next_u64() as u8).collect()
    }

    /// Overwrites a 2- or 4-byte length-like field at a random offset of `buf` with an edge
    /// value (0, max, off-by-one ...), in either byte order.
    pub(crate) fn poke_len(rng: &mut SweepRng, buf: &mut [u8]) {
        if buf.len() < 2 {
            return;
        }
        let at = rng.below(buf.len() - 1);
        let wide = buf.len() - at >= 4 && rng.below(2) == 0;
        let big = rng.below(2) == 0;
        if wide {
            let field: [u8; 4] = [buf[at], buf[at + 1], buf[at + 2], buf[at + 3]];
            let real = if big {
                u32::from_be_bytes(field)
            } else {
                u32::from_le_bytes(field)
            };
            let v = rng.edge_u32(real);
            let b = if big {
                v.to_be_bytes()
            } else {
                v.to_le_bytes()
            };
            buf[at..at + 4].copy_from_slice(&b);
        } else {
            let field = [buf[at], buf[at + 1]];
            let real = if big {
                u16::from_be_bytes(field)
            } else {
                u16::from_le_bytes(field)
            };
            let v = rng.edge_u32(u32::from(real)) as u16;
            let b = if big {
                v.to_be_bytes()
            } else {
                v.to_le_bytes()
            };
            buf[at..at + 2].copy_from_slice(&b);
        }
    }

    /// An arbitrary value, edge cases included (extreme times, NaN, huge decimals).
    pub(crate) fn datum(rng: &mut SweepRng) -> crate::datum::Datum {
        use crate::datum::Datum;
        let big = |rng: &mut SweepRng| match rng.below(4) {
            0 => {
                let edges = [
                    i64::MIN,
                    i64::MIN + 1,
                    i64::MAX,
                    i64::MAX - 1,
                    i64::from(i32::MIN),
                    i64::from(i32::MAX),
                    -1,
                    0,
                    86_400_000_000_000,
                    -86_400_000_000_000,
                ];
                edges[rng.below(edges.len())].wrapping_add(rng.below(3) as i64 - 1)
            }
            1 => rng.below(2000) as i64 - 1000,
            _ => (rng.next_u64() as i64) >> rng.below(64),
        };
        match rng.below(16) {
            0 => Datum::Null,
            1 => Datum::Bool(rng.below(2) == 0),
            2 => Datum::Int(big(rng)),
            3 => {
                let m = (i128::from(big(rng)) << rng.below(64)) | i128::from(rng.next_u64());
                let scale = rng.below(40) as u8;
                storage_types::Decimal::new(m, scale).map_or(Datum::Int(0), Datum::Numeric)
            }
            4 => Datum::Float(f64::from_bits(rng.next_u64())),
            5 => {
                let n = rng.below(8);
                let s = junk(rng, n);
                Datum::Text(String::from_utf8_lossy(&s).into_owned())
            }
            6 => {
                let n = rng.below(8);
                Datum::Bytes(junk(rng, n))
            }
            7 => Datum::Timestamp(big(rng)),
            8 => Datum::Date(big(rng) as i32),
            9 => Datum::Time(big(rng)),
            10 => Datum::Interval {
                months: big(rng),
                days: big(rng),
                ns: big(rng),
            },
            11 => {
                let mut u = [0u8; 16];
                u.iter_mut().for_each(|b| *b = rng.next_u64() as u8);
                Datum::Uuid(u)
            }
            12 => Datum::Json("{\"a\": [1, null]}".into()),
            13 => {
                let n = rng.below(4);
                Datum::Array((0..n).map(|_| datum(rng)).collect())
            }
            14 => {
                let n = rng.below(5);
                Datum::Vector(
                    (0..n)
                        .map(|_| f32::from_bits(rng.next_u64() as u32))
                        .collect(),
                )
            }
            _ => Datum::Text("2024-02-29 23:59:59.999999+05:30".into()),
        }
    }

    /// An arbitrary column type.
    pub(crate) fn data_type(rng: &mut SweepRng) -> crate::ast::DataType {
        use crate::ast::DataType as T;
        match rng.below(24) {
            0 => T::Bool,
            1 => T::SmallInt,
            2 => T::Int,
            3 => T::BigInt,
            4 => T::Numeric(None),
            5 => T::Numeric(Some((rng.below(40) as u8, rng.below(40) as u8))),
            6 => T::Real,
            7 => T::Double,
            8 => T::Text,
            9 => T::Varchar(Some(rng.below(10) as u32)),
            10 => T::Char(Some(rng.below(3) as u32)),
            11 => T::Bytes,
            12 => T::Timestamp,
            13 => T::TimestampTz,
            14 => T::Date,
            15 => T::Time,
            16 => T::Interval,
            17 => T::Uuid,
            18 => T::Json,
            19 => T::Jsonb,
            20 => T::PgChar,
            21 => T::Vector(None),
            22 => T::Name,
            _ => T::Array(Box::new(data_type(rng))),
        }
    }

    /// A hostile variant of `base`: bit flips, edge-valued length fields, truncation, junk.
    pub(crate) fn variant(rng: &mut SweepRng, base: &[u8]) -> Vec<u8> {
        match rng.below(8) {
            0 => {
                let n = rng.below(64);
                junk(rng, n)
            }
            1 | 2 => {
                let mut v = base.to_vec();
                for _ in 0..=rng.below(3) {
                    poke_len(rng, &mut v);
                }
                v
            }
            3 => {
                let mut v = base.to_vec();
                v.truncate(rng.below(v.len() + 1));
                v
            }
            _ => rng.hostile_copy(base),
        }
    }
}
