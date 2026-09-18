use std::{
    collections::BTreeSet,
    hash::{
        DefaultHasher,
        Hash,
        Hasher,
    },
};

/// How much busier than a random other worker a module's home worker may be
/// before the request spills over.
pub const LOAD_SPILL_MARGIN: f64 = 0.2;

#[derive(Clone, Debug, PartialEq)]
pub struct WorkerState {
    pub addr: String,
    pub load: f64,
    pub in_flight: usize,
    pub healthy: bool,
}

// Rendezvous hashing: adding a worker moves ~1/N of modules. DefaultHasher is
// only stable within one process, which is fine: only the conductor hashes.
fn score(module: &str, addr: &str) -> u64 {
    let mut h = DefaultHasher::new();
    (module, addr).hash(&mut h);
    h.finish()
}

/// Picks the module's home worker (warm code caches) unless it is full or
/// clearly busier than one random other worker.
pub fn pick<'a>(
    workers: &'a [WorkerState],
    module: &str,
    exclude: &BTreeSet<String>,
    max_in_flight: usize,
    random: usize,
) -> Option<&'a WorkerState> {
    let candidates: Vec<&WorkerState> = workers
        .iter()
        .filter(|w| w.healthy && !exclude.contains(&w.addr))
        .collect();
    let home = *candidates.iter().max_by_key(|w| score(module, &w.addr))?;
    let others: Vec<&WorkerState> = candidates
        .into_iter()
        .filter(|w| w.addr != home.addr)
        .collect();
    if others.is_empty() {
        return Some(home);
    }
    let other = others[random % others.len()];
    if home.in_flight >= max_in_flight || home.load > other.load + LOAD_SPILL_MARGIN {
        Some(other)
    } else {
        Some(home)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn w(addr: &str, load: f64, in_flight: usize) -> WorkerState {
        WorkerState {
            addr: addr.into(),
            load,
            in_flight,
            healthy: true,
        }
    }

    fn none() -> BTreeSet<String> {
        BTreeSet::new()
    }

    #[test]
    fn no_healthy_worker_returns_none() {
        let mut a = w("a", 0.0, 0);
        a.healthy = false;
        assert!(pick(&[a], "m.js", &none(), 15, 0).is_none());
    }

    #[test]
    fn same_module_goes_to_same_home() {
        let ws = [w("a", 0.1, 0), w("b", 0.1, 0), w("c", 0.1, 0)];
        let first = pick(&ws, "messages.js", &none(), 15, 0)
            .unwrap()
            .addr
            .clone();
        for r in 0..20 {
            assert_eq!(
                pick(&ws, "messages.js", &none(), 15, r).unwrap().addr,
                first
            );
        }
    }

    #[test]
    fn overloaded_home_spills_to_other() {
        let ws = [w("a", 0.0, 0), w("b", 0.0, 0)];
        let home = pick(&ws, "m.js", &none(), 15, 0).unwrap().addr.clone();
        let loaded: Vec<_> = ws
            .iter()
            .map(|x| {
                if x.addr == home {
                    w(&x.addr, 0.9, 0)
                } else {
                    x.clone()
                }
            })
            .collect();
        assert_ne!(pick(&loaded, "m.js", &none(), 15, 0).unwrap().addr, home);
    }

    #[test]
    fn full_home_spills_to_other() {
        let ws = [w("a", 0.0, 0), w("b", 0.0, 0)];
        let home = pick(&ws, "m.js", &none(), 15, 0).unwrap().addr.clone();
        let full: Vec<_> = ws
            .iter()
            .map(|x| {
                if x.addr == home {
                    w(&x.addr, 0.0, 15)
                } else {
                    x.clone()
                }
            })
            .collect();
        assert_ne!(pick(&full, "m.js", &none(), 15, 0).unwrap().addr, home);
    }

    #[test]
    fn single_worker_is_used_even_when_full() {
        let ws = [w("a", 1.0, 99)];
        assert_eq!(pick(&ws, "m.js", &none(), 15, 0).unwrap().addr, "a");
    }

    #[test]
    fn exclude_is_respected() {
        let ws = [w("a", 0.0, 0), w("b", 0.0, 0)];
        let ex = BTreeSet::from(["a".to_string()]);
        assert_eq!(pick(&ws, "m.js", &ex, 15, 0).unwrap().addr, "b");
    }

    #[test]
    fn modules_spread_across_workers() {
        let ws = [
            w("a", 0.0, 0),
            w("b", 0.0, 0),
            w("c", 0.0, 0),
            w("d", 0.0, 0),
        ];
        let mut counts = std::collections::BTreeMap::<String, usize>::new();
        for i in 0..1000 {
            *counts
                .entry(
                    pick(&ws, &format!("mod{i}.js"), &none(), 15, 0)
                        .unwrap()
                        .addr
                        .clone(),
                )
                .or_default() += 1;
        }
        assert!(
            counts.values().all(|&c| (150..=350).contains(&c)),
            "{counts:?}"
        );
    }
}
