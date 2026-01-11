use std::sync::{Arc, Mutex, mpsc};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct DownloadPool {
    sender: mpsc::Sender<Job>,
}

impl DownloadPool {
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let worker_count = size.max(1);
        for _ in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            std::thread::spawn(move || {
                loop {
                    let job = {
                        let receiver = receiver.lock().expect("download pool receiver lock");
                        receiver.recv()
                    };
                    match job {
                        Ok(job) => job(),
                        Err(_) => break,
                    }
                }
            });
        }
        Self { sender }
    }

    pub fn enqueue<F>(&self, job: F) -> Result<(), mpsc::SendError<Job>>
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn pool_limits_concurrency() {
        let pool = DownloadPool::new(2);
        let current = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        for _ in 0..8 {
            let current = Arc::clone(&current);
            let max = Arc::clone(&max);
            let tx = tx.clone();
            pool.enqueue(move || {
                let cur = current.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let prev = max.load(Ordering::SeqCst);
                    if cur > prev {
                        if max
                            .compare_exchange(prev, cur, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(25));
                current.fetch_sub(1, Ordering::SeqCst);
                let _ = tx.send(());
            })
            .expect("enqueue");
        }

        for _ in 0..8 {
            rx.recv_timeout(Duration::from_secs(2))
                .expect("task completion");
        }

        let observed_max = max.load(Ordering::SeqCst);
        assert!(observed_max <= 2);
        assert!(observed_max > 0);
    }
}
