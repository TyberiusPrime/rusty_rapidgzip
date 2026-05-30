//! Dynamic decode-worker controller.
//!
//! The decode pool spawns at a fixed ceiling (`-P`, or `available_parallelism`)
//! and [`crate::pipeline::PoolGate`] can shed workers below it at runtime. This
//! module is the *policy*: given periodic samples of how full the worker →
//! downstream `result` channel is, decide when to shed a worker or restore one.
//!
//! ## Why result-channel fill is the right signal
//!
//! The pool's only job is to keep the slowest downstream stage fed. If the
//! result channel sits **full**, decode is out-producing everything downstream
//! (stage-A / resolve / output / sink) — workers are about to block on `send`,
//! so an active worker is pure waste (extra CPU-churn + in-flight RSS). Shed one.
//! If it sits **empty**, downstream is starved and we may be under-supplying;
//! restore a worker (up to the ceiling).
//!
//! This naturally separates the two regimes the benchmark exposed:
//! * *Slow co-located consumer* → every channel backs up → result full → shed.
//! * *Oversubscribed box, fast consumer* → downstream drains fine → result
//!   never fills → we stay at the ceiling (where contention wants us — fewer
//!   threads there would just cost scheduler share and wall-time).
//!
//! Growth is deliberately more reluctant than shedding (longer streak, bias to
//! stay shed): shedding only ever costs a bounded slice of throughput the
//! downstream couldn't use anyway, while flapping upward wastes the resources
//! we just reclaimed.

/// Tunable thresholds. Defaults are sane for a ~50 ms sample tick; all are
/// overridable from the environment for experimentation (see [`Policy::from_env`]).
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// `occ >= high` ⇒ downstream can't keep up ⇒ candidate to shed.
    pub high: f64,
    /// `occ <= low` ⇒ downstream starved ⇒ candidate to restore.
    pub low: f64,
    /// Consecutive high samples required before shedding one worker.
    pub shed_streak: u32,
    /// Consecutive low samples required before restoring one worker.
    pub grow_streak: u32,
    /// Ticks to ignore samples after any change (debounce / let it settle).
    pub cooldown: u32,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            high: 0.75,
            low: 0.0, // channel must be fully drained to justify growing
            shed_streak: 3,
            grow_streak: 8, // grow back ~2.5× more reluctantly than we shed
            cooldown: 4,
        }
    }
}

impl Policy {
    /// Read overrides from `RAPIDGZIP_AUTOTUNE_{HIGH,LOW,SHED,GROW,COOLDOWN}`.
    pub fn from_env() -> Self {
        let mut p = Self::default();
        let f = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<f64>().ok());
        let u = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<u32>().ok());
        if let Some(v) = f("RAPIDGZIP_AUTOTUNE_HIGH") { p.high = v; }
        if let Some(v) = f("RAPIDGZIP_AUTOTUNE_LOW") { p.low = v; }
        if let Some(v) = u("RAPIDGZIP_AUTOTUNE_SHED") { p.shed_streak = v; }
        if let Some(v) = u("RAPIDGZIP_AUTOTUNE_GROW") { p.grow_streak = v; }
        if let Some(v) = u("RAPIDGZIP_AUTOTUNE_COOLDOWN") { p.cooldown = v; }
        p
    }
}

/// Stateful controller. Feed it one occupancy sample per tick via [`observe`];
/// it returns `Some(new_active)` when the active worker count should change.
///
/// [`observe`]: AutoTune::observe
#[derive(Debug)]
pub struct AutoTune {
    policy: Policy,
    ceiling: usize,
    active: usize,
    high_streak: u32,
    low_streak: u32,
    cooldown: u32,
}

impl AutoTune {
    /// Start wide open (active == ceiling); the controller only sheds from there.
    pub fn new(ceiling: usize, policy: Policy) -> Self {
        Self {
            policy,
            ceiling: ceiling.max(1),
            active: ceiling.max(1),
            high_streak: 0,
            low_streak: 0,
            cooldown: 0,
        }
    }

    pub fn active(&self) -> usize {
        self.active
    }

    /// Feed one result-channel fill fraction in `[0, 1]`. Returns the new active
    /// count iff it changed this tick.
    pub fn observe(&mut self, occ: f64) -> Option<usize> {
        if self.cooldown > 0 {
            self.cooldown -= 1;
            return None;
        }
        if occ >= self.policy.high {
            self.high_streak += 1;
            self.low_streak = 0;
            if self.high_streak >= self.policy.shed_streak && self.active > 1 {
                self.active -= 1;
                self.high_streak = 0;
                self.cooldown = self.policy.cooldown;
                return Some(self.active);
            }
        } else if occ <= self.policy.low {
            self.low_streak += 1;
            self.high_streak = 0;
            if self.low_streak >= self.policy.grow_streak && self.active < self.ceiling {
                self.active += 1;
                self.low_streak = 0;
                self.cooldown = self.policy.cooldown;
                return Some(self.active);
            }
        } else {
            // Mid-band: bleed off streaks so transient blips don't accumulate.
            self.high_streak = self.high_streak.saturating_sub(1);
            self.low_streak = self.low_streak.saturating_sub(1);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast() -> Policy {
        // Short streaks / no cooldown for deterministic stepping in tests.
        Policy { high: 0.75, low: 0.0, shed_streak: 2, grow_streak: 3, cooldown: 0 }
    }

    #[test]
    fn sheds_under_sustained_backpressure_down_to_floor() {
        let mut a = AutoTune::new(4, fast());
        // Each shed needs `shed_streak` (2) high samples.
        assert_eq!(a.observe(1.0), None);
        assert_eq!(a.observe(1.0), Some(3));
        assert_eq!(a.observe(1.0), None);
        assert_eq!(a.observe(1.0), Some(2));
        assert_eq!(a.observe(1.0), None);
        assert_eq!(a.observe(1.0), Some(1));
        // Floors at 1 no matter how long it stays full.
        for _ in 0..10 {
            assert_eq!(a.observe(1.0), None);
        }
        assert_eq!(a.active(), 1);
    }

    #[test]
    fn restores_when_downstream_starves_up_to_ceiling() {
        let mut a = AutoTune::new(4, fast());
        // Shed to 2 first.
        a.observe(1.0);
        a.observe(1.0); // ->3
        a.observe(1.0);
        a.observe(1.0); // ->2
        assert_eq!(a.active(), 2);
        // Empty channel for grow_streak (3) samples restores one.
        assert_eq!(a.observe(0.0), None);
        assert_eq!(a.observe(0.0), None);
        assert_eq!(a.observe(0.0), Some(3));
        // ...and again up to the ceiling, then stops.
        assert_eq!(a.observe(0.0), None);
        assert_eq!(a.observe(0.0), None);
        assert_eq!(a.observe(0.0), Some(4));
        for _ in 0..10 {
            assert_eq!(a.observe(0.0), None);
        }
        assert_eq!(a.active(), 4);
    }

    #[test]
    fn mid_band_holds_steady() {
        let mut a = AutoTune::new(8, fast());
        for _ in 0..50 {
            assert_eq!(a.observe(0.4), None);
        }
        assert_eq!(a.active(), 8);
    }

    #[test]
    fn cooldown_debounces_changes() {
        let p = Policy { high: 0.75, low: 0.0, shed_streak: 1, grow_streak: 1, cooldown: 3 };
        let mut a = AutoTune::new(8, p);
        assert_eq!(a.observe(1.0), Some(7)); // sheds immediately (streak 1)
        // Next 3 ticks are swallowed by cooldown even though still full.
        assert_eq!(a.observe(1.0), None);
        assert_eq!(a.observe(1.0), None);
        assert_eq!(a.observe(1.0), None);
        assert_eq!(a.observe(1.0), Some(6));
    }

    #[test]
    fn streak_decay_resists_flapping() {
        let mut a = AutoTune::new(4, fast());
        // One high sample, then mid-band wipes the partial streak, so the next
        // lone high sample can't tip a shed on its own.
        assert_eq!(a.observe(1.0), None); // high_streak=1
        assert_eq!(a.observe(0.4), None); // decays to 0
        assert_eq!(a.observe(1.0), None); // high_streak=1 again, no shed yet
        assert_eq!(a.active(), 4);
    }
}
