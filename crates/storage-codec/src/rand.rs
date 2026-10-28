//! Cryptographically secure random bytes from the operating system (key generation, TLS).

use std::io;

/// Fills `buf` from the OS CSPRNG: `/dev/urandom` on Unix (never blocks once the kernel pool
/// is initialized), `BCryptGenRandom` on Windows.
pub fn os_random(buf: &mut [u8]) -> io::Result<()> {
    imp::fill(buf)
}

/// `N` random bytes from the OS CSPRNG.
pub fn os_random_array<const N: usize>() -> io::Result<[u8; N]> {
    let mut out = [0u8; N];
    os_random(&mut out)?;
    Ok(out)
}

#[cfg(unix)]
mod imp {
    use std::fs::File;
    use std::io::{self, Read};

    pub fn fill(buf: &mut [u8]) -> io::Result<()> {
        File::open("/dev/urandom")?.read_exact(buf)
    }
}

#[cfg(windows)]
mod imp {
    use std::ffi::c_void;
    use std::io;

    #[link(name = "bcrypt")]
    extern "system" {
        fn BCryptGenRandom(algorithm: *mut c_void, buffer: *mut u8, len: u32, flags: u32) -> i32;
    }

    const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x0000_0002;

    pub fn fill(buf: &mut [u8]) -> io::Result<()> {
        for chunk in buf.chunks_mut(u32::MAX as usize) {
            // SAFETY: `chunk` is a valid, writable buffer of exactly `chunk.len()` bytes for the
            // duration of the call; a null algorithm handle is allowed with
            // BCRYPT_USE_SYSTEM_PREFERRED_RNG, and the function writes only within the buffer.
            let status = unsafe {
                BCryptGenRandom(
                    std::ptr::null_mut(),
                    chunk.as_mut_ptr(),
                    chunk.len() as u32,
                    BCRYPT_USE_SYSTEM_PREFERRED_RNG,
                )
            };
            if status != 0 {
                return Err(io::Error::other(format!(
                    "BCryptGenRandom failed with status {status:#x}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
mod imp {
    use std::io;

    pub fn fill(_buf: &mut [u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no OS random source on this platform",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_random_fills_and_differs() {
        let a: [u8; 32] = os_random_array().unwrap();
        let b: [u8; 32] = os_random_array().unwrap();
        assert_ne!(a, b);
        assert_ne!(a, [0; 32]);
    }
}
