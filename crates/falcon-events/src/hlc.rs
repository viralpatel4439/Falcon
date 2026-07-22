//! Hybrid Logical Clock (HLC) for multi-region last-write-wins ordering.
//!
//! An HLC combines physical wall-clock time with a logical counter so that
//! events across regions get a deterministic total order that stays close
//! to real time, without requiring synchronized clocks. Ordering is
//! lexicographic on `(wall, logical, region)` — `region` is the final
//! tiebreak so two regions never produce equal, incomparable stamps.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::Mutex;

fn wall_now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hlc {
    pub wall: u64,
    pub logical: u32,
    pub region: String,
}

impl Hlc {
    pub fn zero() -> Self {
        Self {
            wall: 0,
            logical: 0,
            region: String::new(),
        }
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wall
            .cmp(&other.wall)
            .then(self.logical.cmp(&other.logical))
            .then_with(|| self.region.cmp(&other.region))
    }
}

/// Generates monotonic HLCs for a region and advances on observing remote
/// stamps, keeping regions loosely synchronized without NTP.
pub struct HlcClock {
    region: String,
    state: Mutex<(u64, u32)>, // (last wall, last logical)
}

impl HlcClock {
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            state: Mutex::new((0, 0)),
        }
    }

    /// Produce the next local HLC (for a local write). Monotonic: if the
    /// wall clock hasn't advanced past the last stamp, bump the logical
    /// counter instead.
    pub fn now(&self) -> Hlc {
        let mut st = self.state.lock().expect("hlc mutex poisoned");
        let phys = wall_now_millis();
        if phys > st.0 {
            *st = (phys, 0);
        } else {
            st.1 += 1;
        }
        Hlc {
            wall: st.0,
            logical: st.1,
            region: self.region.clone(),
        }
    }

    /// Advance the clock on observing a remote HLC (e.g. from a replicated
    /// event), so this region's future stamps stay ahead of what it has
    /// seen. Returns a fresh local HLC reflecting the merge.
    pub fn observe(&self, remote: &Hlc) -> Hlc {
        let mut st = self.state.lock().expect("hlc mutex poisoned");
        let phys = wall_now_millis();
        let max_wall = phys.max(st.0).max(remote.wall);
        let logical = if max_wall == st.0 && max_wall == remote.wall {
            st.1.max(remote.logical) + 1
        } else if max_wall == st.0 {
            st.1 + 1
        } else if max_wall == remote.wall {
            remote.logical + 1
        } else {
            0
        };
        *st = (max_wall, logical);
        Hlc {
            wall: max_wall,
            logical,
            region: self.region.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_monotonic() {
        let clock = HlcClock::new("r1");
        let mut prev = clock.now();
        for _ in 0..1000 {
            let next = clock.now();
            assert!(next > prev, "HLC must be strictly monotonic: {prev:?} !< {next:?}");
            prev = next;
        }
    }

    #[test]
    fn total_order_breaks_ties_by_region() {
        let a = Hlc { wall: 5, logical: 2, region: "a".into() };
        let b = Hlc { wall: 5, logical: 2, region: "b".into() };
        assert!(a < b); // same wall+logical -> region breaks the tie
        assert_ne!(a, b);
    }

    #[test]
    fn observe_advances_past_remote() {
        let clock = HlcClock::new("local");
        let remote = Hlc { wall: u64::MAX / 2, logical: 99, region: "remote".into() };
        let merged = clock.observe(&remote);
        assert!(merged > remote, "after observing, local clock must be ahead of remote");
        // And subsequent local stamps stay ahead too.
        let next = clock.now();
        assert!(next > remote);
    }

    #[test]
    fn wall_dominates_logical() {
        let lo = Hlc { wall: 10, logical: 999, region: "z".into() };
        let hi = Hlc { wall: 11, logical: 0, region: "a".into() };
        assert!(hi > lo);
    }
}
