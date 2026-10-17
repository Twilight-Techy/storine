//! Online Lazy Schema Evolution & Multi-Step Upcasting Pipeline (`TASK-033`, `F-CAT-03`, `F-OPS-01`, `RULE-CPU-04`).
//!
//! Supports both:
//! 1. Direct schema migrators (`old_version -> T`)
//! 2. Chained step-by-step payload upcasters (`v1 -> v2 -> v3 -> ... -> T::SCHEMA_VERSION`)
//!
//! Marked `#[cold]` and `#[inline(never)]` on the migration execution path so the hot read path
//! (`hdr.schema_version == T::SCHEMA_VERSION`) remains branch-predicted and 0-alloc (`187 ns`).

use storage_codec::ZeroCopyRecord;
use storage_types::{FastHashMap, Result, StorageError};

/// Step-by-step byte transformation upcasting a payload from `from_version` to `from_version + 1`.
pub type StepUpcasterFn = Box<dyn Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync>;

/// Direct schema migration function converting `old_version` payload bytes into current `T`.
pub type DirectMigrationFn<T> = Box<dyn Fn(u16, &[u8]) -> Result<T> + Send + Sync>;

/// Registry of schema migrations and step-by-step upcasters for a collection `T`.
#[derive(Default)]
pub struct SchemaRegistry<T: ZeroCopyRecord> {
    direct: FastHashMap<u16, DirectMigrationFn<T>>,
    step_upcasters: FastHashMap<u16, StepUpcasterFn>,
    final_decoder: Option<DirectMigrationFn<T>>,
}

impl<T: ZeroCopyRecord> core::fmt::Debug for SchemaRegistry<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SchemaRegistry")
            .field("direct_versions", &self.direct.keys().collect::<Vec<_>>())
            .field(
                "step_versions",
                &self.step_upcasters.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<T: ZeroCopyRecord> SchemaRegistry<T> {
    #[inline]
    pub fn new() -> Self {
        Self {
            direct: FastHashMap::default(),
            step_upcasters: FastHashMap::default(),
            final_decoder: None,
        }
    }

    /// Registers a direct migration from `old_schema_version` directly to `T`.
    pub fn register_direct(
        &mut self,
        old_schema_version: u16,
        migrator: impl Fn(u16, &[u8]) -> Result<T> + Send + Sync + 'static,
    ) {
        self.direct.insert(old_schema_version, Box::new(migrator));
    }

    /// Registers a step upcaster transforming raw payload bytes from `from_version` to `from_version + 1`.
    pub fn register_step(
        &mut self,
        from_version: u16,
        step_fn: impl Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync + 'static,
    ) {
        self.step_upcasters.insert(from_version, Box::new(step_fn));
    }

    /// Registers a final decoder for step-upcasted bytes once they reach `T::SCHEMA_VERSION`.
    pub fn set_final_decoder(
        &mut self,
        decoder: impl Fn(u16, &[u8]) -> Result<T> + Send + Sync + 'static,
    ) {
        self.final_decoder = Some(Box::new(decoder));
    }

    /// Cold-path schema migration executor (`RULE-CPU-04`).
    #[cold]
    #[inline(never)]
    pub fn migrate_payload(&self, found_version: u16, raw_payload: &[u8]) -> Result<T> {
        // 1. Check direct migrator first (O(1) single-step conversion)
        if let Some(mig_fn) = self.direct.get(&found_version) {
            return mig_fn(found_version, raw_payload);
        }

        // 2. Otherwise walk the step-by-step upcaster chain `found_version -> ... -> T::SCHEMA_VERSION`
        if found_version < T::SCHEMA_VERSION && !self.step_upcasters.is_empty() {
            let mut cur_ver = found_version;
            let mut cur_bytes: Vec<u8> = raw_payload.to_vec();
            while cur_ver < T::SCHEMA_VERSION {
                if let Some(direct_fn) = self.direct.get(&cur_ver) {
                    return direct_fn(cur_ver, &cur_bytes);
                }
                let Some(step_fn) = self.step_upcasters.get(&cur_ver) else {
                    return Err(StorageError::SchemaVersionMismatch {
                        found: cur_ver,
                        expected: T::SCHEMA_VERSION,
                    });
                };
                cur_bytes = step_fn(&cur_bytes)?;
                cur_ver = cur_ver.saturating_add(1);
            }
            if let Some(ref dec) = self.final_decoder {
                return dec(T::SCHEMA_VERSION, &cur_bytes);
            }
        }

        Err(StorageError::SchemaVersionMismatch {
            found: found_version,
            expected: T::SCHEMA_VERSION,
        })
    }
}
