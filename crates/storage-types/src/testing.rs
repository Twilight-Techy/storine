//! Deterministic hostile-input generator shared by every crate's corruption / fuzz sweeps
//! (`RULE-TEST-01`, `RULE-ERR-01`). Not part of the stable API; unused code is removed by LTO.

/// Iteration multiplier for fuzz and hostile-input sweeps (`STORINE_FUZZ_ITERS`, default 1).
/// The long runs (`TASK-135`) use 100+ in release with debug assertions on.
pub fn fuzz_scale() -> usize {
    std::env::var("STORINE_FUZZ_ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1)
}

/// Reproducible xorshift64* generator with mutation helpers for decoder sweeps.
#[derive(Debug, Clone)]
pub struct SweepRng(u64);

impl SweepRng {
    /// Creates a generator from a non-zero seed (zero is mapped to a fixed constant).
    pub const fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish value in `0..n` (`0` when `n == 0`).
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % (n.max(1) as u64)) as usize
    }

    /// Edge-case `u32` derived from `real` (zero, max, off-by-one, doubled, power of two, random).
    pub fn edge_u32(&mut self, real: u32) -> u32 {
        match self.below(8) {
            0 => 0,
            1 => u32::MAX,
            2 => real.wrapping_add(1),
            3 => real.wrapping_sub(1),
            4 => real.wrapping_mul(2),
            5 => 1 << self.below(32),
            6 => self.next_u64() as u32,
            _ => real,
        }
    }

    /// Edge-case `u64` derived from `real`.
    pub fn edge_u64(&mut self, real: u64) -> u64 {
        match self.below(6) {
            0 => 0,
            1 => u64::MAX,
            2 => real.wrapping_add(1),
            3 => real.wrapping_sub(1),
            4 => self.next_u64(),
            _ => real,
        }
    }

    /// Flips a bit, overwrites a byte, or splats `0xFF` over up to 4 bytes, 1–4 times.
    pub fn mutate(&mut self, buf: &mut [u8]) {
        if buf.is_empty() {
            return;
        }
        for _ in 0..=self.below(4) {
            let at = self.below(buf.len());
            match self.below(3) {
                0 => buf[at] ^= 1 << self.below(8),
                1 => buf[at] = self.next_u64() as u8,
                _ => {
                    let end = (at + 4).min(buf.len());
                    buf[at..end].fill(0xFF);
                }
            }
        }
    }

    /// Mutated copy of `base`, sometimes truncated or extended with junk.
    pub fn hostile_copy(&mut self, base: &[u8]) -> Vec<u8> {
        let mut out = base.to_vec();
        self.mutate(&mut out);
        match self.below(6) {
            0 => out.truncate(self.below(out.len() + 1)),
            1 => {
                let extra = self.below(64);
                out.extend((0..extra).map(|_| self.next_u64() as u8));
            }
            _ => {}
        }
        out
    }
}
