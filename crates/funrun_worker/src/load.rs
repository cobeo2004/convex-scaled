//! Load reported to the conductor so it can pick the least busy worker.

use common::knobs::{
    FUNRUN_MAX_CPU_PRESSURE,
    FUNRUN_TARGET_CPU_USAGE,
    FUNRUN_TARGET_ISOLATE_WORKER_USAGE,
};

pub struct LoadInputs {
    pub in_flight: usize,
    pub max_isolate_workers: usize,
    pub cpu_util: f64,
    pub cpu_psi: Option<f64>,
}

pub struct LoadTargets {
    pub isolate: f64,
    pub cpu: f64,
    pub psi: f64,
}

impl LoadTargets {
    pub fn from_knobs() -> Self {
        Self {
            isolate: *FUNRUN_TARGET_ISOLATE_WORKER_USAGE,
            cpu: *FUNRUN_TARGET_CPU_USAGE,
            psi: *FUNRUN_MAX_CPU_PRESSURE,
        }
    }
}

/// Same shape as upstream Funrun's load reporter: each signal is divided by
/// its target and the worst one wins, capped at 1.0.
pub fn effective_load(i: &LoadInputs, t: &LoadTargets) -> f64 {
    let isolates = i.in_flight as f64 / i.max_isolate_workers.max(1) as f64 / t.isolate;
    let cpu = i.cpu_util / t.cpu;
    let psi = i.cpu_psi.map_or(0.0, |p| p / t.psi);
    isolates.max(cpu).max(psi).min(1.0)
}

/// `/proc/pressure/cpu` "some" line -> fraction (avg10 is a percentage).
pub fn parse_psi_avg10(line: &str) -> Option<f64> {
    line.split_whitespace()
        .find_map(|f| f.strip_prefix("avg10="))
        .and_then(|v| v.parse::<f64>().ok())
        .map(|pct| pct / 100.0)
}

// ponytail: Linux /proc only (workers run in Linux containers). Elsewhere
// cpu_util reads 0.0 and PSI None, so load falls back to the in-flight signal.
// /proc/stat and /proc/pressure/cpu are host-wide, not per cgroup; read
// /sys/fs/cgroup/cpu.stat and cpu.pressure instead if workers get CPU quotas.
// The reads are tiny blocking fs calls, done once per report interval.
pub struct CpuSample {
    /// CPU utilisation since the previous sample, in [0, 1].
    pub util: f64,
    /// PSI "some" avg10 as a fraction, when the kernel exposes it.
    pub psi: Option<f64>,
}

#[derive(Default)]
pub struct CpuSampler {
    prev: Option<(u64, u64)>, // (idle, total) jiffies
}

impl CpuSampler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sample(&mut self) -> CpuSample {
        let psi = std::fs::read_to_string("/proc/pressure/cpu")
            .ok()
            .and_then(|s| s.lines().next().and_then(parse_psi_avg10));
        let Some((idle, total)) = read_proc_stat() else {
            return CpuSample { util: 0.0, psi };
        };
        let util = match self.prev.replace((idle, total)) {
            Some((pi, pt)) if total > pt => {
                1.0 - idle.saturating_sub(pi) as f64 / (total - pt) as f64
            },
            _ => 0.0,
        };
        CpuSample { util, psi }
    }
}

fn read_proc_stat() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let cpu: Vec<u64> = stat
        .lines()
        .next()?
        .split_whitespace()
        .skip(1)
        // guest and guest_nice (fields 9-10) are already counted in user/nice.
        .take(8)
        .filter_map(|v| v.parse().ok())
        .collect();
    let idle = cpu.get(3)? + cpu.get(4).copied().unwrap_or(0); // idle + iowait
    Some((idle, cpu.iter().sum()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: LoadTargets = LoadTargets {
        isolate: 0.75,
        cpu: 0.90,
        psi: 0.20,
    };

    fn inputs(in_flight: usize, cpu: f64, psi: Option<f64>) -> LoadInputs {
        LoadInputs {
            in_flight,
            max_isolate_workers: 100,
            cpu_util: cpu,
            cpu_psi: psi,
        }
    }

    #[test]
    fn idle_is_zero() {
        assert_eq!(effective_load(&inputs(0, 0.0, None), &T), 0.0);
    }

    #[test]
    fn max_of_signals_normalised_by_target() {
        let l = effective_load(&inputs(30, 0.45, Some(0.02)), &T); // 0.4, 0.5, 0.1
        assert!((l - 0.5).abs() < 1e-9);
    }

    #[test]
    fn psi_ignored_when_absent_and_saturates_at_one() {
        assert_eq!(effective_load(&inputs(0, 0.0, Some(0.5)), &T), 1.0);
        assert_eq!(effective_load(&inputs(200, 0.0, None), &T), 1.0);
    }

    #[test]
    fn parses_proc_pressure_line() {
        let line = "some avg10=12.50 avg60=3.00 avg300=1.00 total=123";
        assert_eq!(parse_psi_avg10(line), Some(0.125));
    }
}
