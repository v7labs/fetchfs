use std::sync::{Condvar, Mutex};

pub struct DownloadGate {
    max: usize,
    state: Mutex<usize>,
    condvar: Condvar,
}

impl DownloadGate {
    pub fn new(max: usize) -> Self {
        DownloadGate {
            max: max.max(1),
            state: Mutex::new(0),
            condvar: Condvar::new(),
        }
    }

    pub fn acquire(&self) -> DownloadPermit<'_> {
        let mut in_flight = self.state.lock().expect("download gate lock");
        while *in_flight >= self.max {
            in_flight = self.condvar.wait(in_flight).expect("download gate wait");
        }
        *in_flight += 1;
        DownloadPermit { gate: self }
    }

    fn release(&self) {
        let mut in_flight = self.state.lock().expect("download gate lock");
        *in_flight = in_flight.saturating_sub(1);
        self.condvar.notify_one();
    }
}

pub struct DownloadPermit<'a> {
    gate: &'a DownloadGate,
}

impl Drop for DownloadPermit<'_> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn acquire_blocks_when_max_reached() {
        let gate = std::sync::Arc::new(DownloadGate::new(1));
        let _first = gate.acquire();
        let (tx, rx) = mpsc::channel();

        let gate_ref = std::sync::Arc::clone(&gate);
        thread::spawn(move || {
            let _second = gate_ref.acquire();
            let _ = tx.send(());
        });

        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn acquire_releases_when_permit_dropped() {
        let gate = std::sync::Arc::new(DownloadGate::new(1));
        let first = gate.acquire();
        let (tx, rx) = mpsc::channel();

        let gate_ref = std::sync::Arc::clone(&gate);
        thread::spawn(move || {
            let _second = gate_ref.acquire();
            let _ = tx.send(());
        });

        drop(first);
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
    }
}
