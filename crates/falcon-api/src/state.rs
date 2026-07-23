use falcon_core::{FeatureSet, Node};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub node: Arc<Node>,
    /// The Falcon products active on this node (from its profile). Drives route
    /// gating, the `/health` feature report, and which UI is served at `/`.
    pub features: Arc<FeatureSet>,
    /// Path to the profile file, so the UI's `POST /config` can persist edits
    /// through the same CLI/UI-only config path (never env vars).
    pub profile_path: Arc<PathBuf>,
}
