//! AES rounds on AES-NI: the unsafe half of the AES in `storage-sql` (`RULE-SAFE-01`). Each call
//! checks the CPU itself, so no caller can reach the instructions on a CPU without them.

/// One block with expanded `keys`; `false`, leaving `b` alone, without AES-NI or with too few keys.
#[cfg(target_arch = "x86_64")]
pub fn encrypt(keys: &[[u8; 16]], b: &mut [u8; 16]) -> bool {
    if keys.len() < 2 || !detected() {
        return false;
    }
    // SAFETY: AES-NI and SSE2 were detected just above, and `keys` holds at least two round keys.
    unsafe { hardware::encrypt(keys, b) };
    true
}

/// The inverse of `encrypt`, with the same `keys`.
#[cfg(target_arch = "x86_64")]
pub fn decrypt(keys: &[[u8; 16]], b: &mut [u8; 16]) -> bool {
    if keys.len() < 2 || !detected() {
        return false;
    }
    // SAFETY: AES-NI and SSE2 were detected just above, and `keys` holds at least two round keys.
    unsafe { hardware::decrypt(keys, b) };
    true
}

#[cfg(not(target_arch = "x86_64"))]
pub fn encrypt(_keys: &[[u8; 16]], _b: &mut [u8; 16]) -> bool {
    false
}

#[cfg(not(target_arch = "x86_64"))]
pub fn decrypt(_keys: &[[u8; 16]], _b: &mut [u8; 16]) -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
fn detected() -> bool {
    std::arch::is_x86_feature_detected!("aes") && std::arch::is_x86_feature_detected!("sse2")
}

#[cfg(target_arch = "x86_64")]
mod hardware {
    use std::arch::x86_64::*;

    /// # Safety
    /// The CPU has AES-NI and SSE2, and `keys.len() >= 2`.
    #[target_feature(enable = "aes,sse2")]
    pub unsafe fn encrypt(keys: &[[u8; 16]], b: &mut [u8; 16]) {
        let rounds = keys.len() - 1;
        // SAFETY: every pointer is to a 16-byte array (unaligned `loadu` / `storeu` are allowed);
        // the caller vouches for the CPU features, which this function enables.
        unsafe {
            let load = |k: &[u8; 16]| _mm_loadu_si128(k.as_ptr().cast());
            let mut s = _mm_xor_si128(_mm_loadu_si128(b.as_ptr().cast()), load(&keys[0]));
            for k in &keys[1..rounds] {
                s = _mm_aesenc_si128(s, load(k));
            }
            s = _mm_aesenclast_si128(s, load(&keys[rounds]));
            _mm_storeu_si128(b.as_mut_ptr().cast(), s);
        }
    }

    /// # Safety
    /// As for `encrypt`.
    #[target_feature(enable = "aes,sse2")]
    pub unsafe fn decrypt(keys: &[[u8; 16]], b: &mut [u8; 16]) {
        let rounds = keys.len() - 1;
        // SAFETY: as in `encrypt`.
        unsafe {
            let load = |k: &[u8; 16]| _mm_loadu_si128(k.as_ptr().cast());
            let mut s = _mm_xor_si128(_mm_loadu_si128(b.as_ptr().cast()), load(&keys[rounds]));
            for k in keys[1..rounds].iter().rev() {
                s = _mm_aesdec_si128(s, _mm_aesimc_si128(load(k)));
            }
            s = _mm_aesdeclast_si128(s, load(&keys[0]));
            _mm_storeu_si128(b.as_mut_ptr().cast(), s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_few_keys_are_refused_not_undefined() {
        let mut b = [7u8; 16];
        assert!(!encrypt(&[], &mut b));
        assert!(!encrypt(&[[0; 16]], &mut b));
        assert!(!decrypt(&[[0; 16]], &mut b));
        assert_eq!(b, [7u8; 16]);
    }
}
