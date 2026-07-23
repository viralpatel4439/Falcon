//! Bridges compile-time Cargo features to the runtime [`FeatureSet`].
//!
//! Each `feat-*` Cargo feature, when enabled, contributes its product to the
//! set the running binary reports as "compiled in". The default `full` build
//! turns them all on; a slim build turns on exactly one.

use falcon_core::{Feature, FeatureSet};

/// The products this binary was compiled with.
pub fn compiled() -> FeatureSet {
    let mut set = FeatureSet::new();
    if cfg!(feature = "feat-cache") {
        set.insert(Feature::Cache);
    }
    if cfg!(feature = "feat-kv") {
        set.insert(Feature::Kv);
    }
    if cfg!(feature = "feat-pubsub") {
        set.insert(Feature::Pubsub);
    }
    if cfg!(feature = "feat-queue") {
        set.insert(Feature::Queue);
    }
    if cfg!(feature = "feat-stream") {
        set.insert(Feature::Stream);
    }
    set
}

/// Human label for the build: the single product name for a slim build, or
/// "Falcon (full)" when every product is compiled in.
pub fn build_label() -> String {
    let c = compiled();
    if c == FeatureSet::all() {
        "Falcon (full)".to_string()
    } else if c.len() == 1 {
        c.iter().next().unwrap().product_name().to_string()
    } else {
        format!("Falcon ({c})")
    }
}
