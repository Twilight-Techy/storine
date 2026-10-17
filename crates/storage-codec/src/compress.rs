//! Fast LZ4-style block compression and decompression for cold records (`TASK-027`, `F-OPS-03`).
//!
//! Uses a 4096-entry 4-byte hash table on the stack/scratch for single-pass $O(N)$ compression
//! and bounds-checked copy loops for safe decompression.

use storage_types::{Result, StorageError, MAX_PAYLOAD_SIZE};

const HASH_LOG: usize = 12;
const HASH_TABLE_SIZE: usize = 1 << HASH_LOG; // 4096 entries (16 KB on stack)
const MIN_MATCH: usize = 4;
const MAX_DISTANCE: usize = 65_535;

/// Compresses `input` into `out` using an LZ4-compatible token+literal+match block format.
pub fn compress_lz4(input: &[u8]) -> Vec<u8> {
    if input.len() < 16 {
        let mut out = Vec::with_capacity(input.len() + 1);
        out.push(0x00); // Raw uncompressed short block marker
        out.extend_from_slice(input);
        return out;
    }

    let mut out = Vec::with_capacity(input.len() + (input.len() / 255) + 16);
    out.push(0x01); // Compressed block marker
    let mut hash_table = [0u32; HASH_TABLE_SIZE];

    let len = input.len();
    let match_limit = len.saturating_sub(5);
    let mut anchor = 0usize;
    let mut ip = 0usize;

    while ip < match_limit {
        let seq = u32::from_le_bytes(input[ip..ip + 4].try_into().unwrap());
        let h = ((seq.wrapping_mul(2_654_435_761)) >> (32 - HASH_LOG)) as usize;
        let ref_pos = hash_table[h] as usize;
        hash_table[h] = ip as u32;

        let dist = ip.saturating_sub(ref_pos);
        if dist > 0
            && dist <= MAX_DISTANCE
            && input[ref_pos..ref_pos + 4] == input[ip..ip + 4]
        {
            // Found a 4-byte match at `ref_pos`
            let lit_len = ip - anchor;
            let mut match_len = MIN_MATCH;
            while ip + match_len < len
                && input[ref_pos + match_len] == input[ip + match_len]
            {
                match_len += 1;
            }

            let lit_token = lit_len.min(15) as u8;
            let match_extra = match_len - MIN_MATCH;
            let match_token = match_extra.min(15) as u8;
            out.push((lit_token << 4) | match_token);

            if lit_len >= 15 {
                write_len_bytes(&mut out, lit_len - 15);
            }
            out.extend_from_slice(&input[anchor..ip]);

            // 16-bit little-endian offset
            out.extend_from_slice(&(dist as u16).to_le_bytes());

            if match_extra >= 15 {
                write_len_bytes(&mut out, match_extra - 15);
            }

            ip += match_len;
            anchor = ip;
        } else {
            ip += 1;
        }
    }

    // Emit trailing literals
    let tail_lit = len - anchor;
    let lit_token = tail_lit.min(15) as u8;
    out.push(lit_token << 4);
    if tail_lit >= 15 {
        write_len_bytes(&mut out, tail_lit - 15);
    }
    out.extend_from_slice(&input[anchor..len]);
    out
}

#[inline]
fn write_len_bytes(out: &mut Vec<u8>, mut rem: usize) {
    while rem >= 255 {
        out.push(255);
        rem -= 255;
    }
    out.push(rem as u8);
}

/// Decompresses an LZ4-encoded slice into a buffer of exact `expected_uncompressed_len` bytes.
/// Protects against decompression bombs and out-of-bounds match offsets.
pub fn decompress_lz4(input: &[u8], expected_uncompressed_len: usize) -> Result<Vec<u8>> {
    if expected_uncompressed_len > MAX_PAYLOAD_SIZE as usize {
        return Err(StorageError::invalid_archive(
            "decompressed length exceeds 256 MiB ceiling",
        ));
    }
    if input.is_empty() {
        if expected_uncompressed_len == 0 {
            return Ok(Vec::new());
        }
        return Err(StorageError::invalid_archive(
            "empty compressed input for non-empty output",
        ));
    }

    let mode = input[0];
    let body = &input[1..];
    if mode == 0x00 {
        if body.len() != expected_uncompressed_len {
            return Err(StorageError::invalid_archive(
                "raw block length mismatch during decompression",
            ));
        }
        return Ok(body.to_vec());
    }
    if mode != 0x01 {
        return Err(StorageError::invalid_archive(
            "unknown compression block marker",
        ));
    }

    let mut out = Vec::with_capacity(expected_uncompressed_len);
    let mut pos = 0usize;

    while pos < body.len() {
        let token = body[pos];
        pos += 1;

        let mut lit_len = (token >> 4) as usize;
        if lit_len == 15 {
            loop {
                if pos >= body.len() {
                    return Err(StorageError::invalid_archive(
                        "truncated literal length in compressed payload",
                    ));
                }
                let b = body[pos] as usize;
                pos += 1;
                lit_len = lit_len
                    .checked_add(b)
                    .ok_or_else(|| StorageError::invalid_archive("literal length overflow"))?;
                if b != 255 {
                    break;
                }
            }
        }

        if pos.checked_add(lit_len).map_or(true, |end| end > body.len()) {
            return Err(StorageError::invalid_archive(
                "literal run exceeds compressed input bounds",
            ));
        }
        if out
            .len()
            .checked_add(lit_len)
            .map_or(true, |end| end > expected_uncompressed_len)
        {
            return Err(StorageError::invalid_archive(
                "decompressed literals exceed expected output length",
            ));
        }
        out.extend_from_slice(&body[pos..pos + lit_len]);
        pos += lit_len;

        if pos >= body.len() {
            break;
        }

        if pos + 2 > body.len() {
            return Err(StorageError::invalid_archive(
                "truncated match offset in compressed payload",
            ));
        }
        let dist = u16::from_le_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;
        if dist == 0 || dist > out.len() {
            return Err(StorageError::invalid_archive(
                "invalid back-reference distance in compressed payload",
            ));
        }

        let mut match_len = ((token & 0x0F) as usize) + MIN_MATCH;
        if (token & 0x0F) == 15 {
            loop {
                if pos >= body.len() {
                    return Err(StorageError::invalid_archive(
                        "truncated match length in compressed payload",
                    ));
                }
                let b = body[pos] as usize;
                pos += 1;
                match_len = match_len
                    .checked_add(b)
                    .ok_or_else(|| StorageError::invalid_archive("match length overflow"))?;
                if b != 255 {
                    break;
                }
            }
        }

        if out
            .len()
            .checked_add(match_len)
            .map_or(true, |end| end > expected_uncompressed_len)
        {
            return Err(StorageError::invalid_archive(
                "decompressed match exceeds expected output length",
            ));
        }

        let match_start = out.len() - dist;
        if dist >= match_len {
            out.extend_from_within(match_start..match_start + match_len);
        } else {
            for i in 0..match_len {
                let b = out[match_start + i];
                out.push(b);
            }
        }
    }

    if out.len() != expected_uncompressed_len {
        return Err(StorageError::invalid_archive(
            "final decompressed length did not match header uncompressed_len",
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip_and_corruption_defense() {
        let mut original = Vec::new();
        for i in 0..500 {
            original.extend_from_slice(b"storage-engine-zero-copy-mmap-record-");
            original.extend_from_slice(&(i as u32).to_le_bytes());
        }
        let compressed = compress_lz4(&original);
        assert!(compressed.len() < original.len() / 3);
        let decompressed = decompress_lz4(&compressed, original.len()).unwrap();
        assert_eq!(original, decompressed);

        // Corrupting expected length must fail cleanly
        assert!(decompress_lz4(&compressed, original.len() - 5).is_err());
    }
}
