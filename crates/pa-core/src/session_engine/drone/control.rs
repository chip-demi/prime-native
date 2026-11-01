//! One assessment's cancellation and synchronous provider-admission boundary.
use pa_agent::abort::AbortController;
use std::sync::{Arc, Mutex};

/// Cancellation belongs to one observer assessment, never to its root's active model turn.
/// The short admission lock prevents a cancelled assessment from starting another transport.
#[derive(Clone, Debug, Default)]
pub struct AssessmentControl {
    abort: AbortController,
    provider_admission: Arc<Mutex<()>>,
}
impl AssessmentControl {
    /// Cancel this assessment. A provider admitted earlier must still confirm settlement.
    pub fn cancel(&self) {
        let _admission = self
            .provider_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.abort.abort();
    }
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.abort.is_aborted()
    }
    pub async fn cancelled(&self) {
        self.abort.signal().aborted().await;
    }
    pub(super) fn admit<T>(&self, start: impl FnOnce() -> anyhow::Result<T>) -> anyhow::Result<T> {
        let _admission = self
            .provider_admission
            .lock()
            .map_err(|_| anyhow::anyhow!("drone_control_unavailable"))?;
        anyhow::ensure!(!self.is_cancelled(), "drone_control_stopped");
        start()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stopped_assessment_cannot_admit_provider_work() {
        let control = AssessmentControl::default();
        control.cancel();
        let started = std::sync::atomic::AtomicUsize::new(0);
        assert!(control
            .admit(|| {
                started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .is_err());
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    #[test]
    fn provider_admission_and_stop_have_one_observable_boundary() {
        let control = AssessmentControl::default();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let admitted_control = control.clone();
        let admitted = std::thread::spawn(move || {
            admitted_control.admit(|| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok("previously admitted")
            })
        });
        entered_rx.recv().unwrap();
        let (cancelled_tx, cancelled_rx) = std::sync::mpsc::channel();
        let stopped_control = control.clone();
        let stopped = std::thread::spawn(move || {
            stopped_control.cancel();
            cancelled_tx.send(()).unwrap();
        });
        assert!(
            cancelled_rx.try_recv().is_err(),
            "stop cannot acknowledge over an unobserved admission"
        );
        release_tx.send(()).unwrap();
        assert_eq!(admitted.join().unwrap().unwrap(), "previously admitted");
        cancelled_rx.recv().unwrap();
        stopped.join().unwrap();
        assert!(control.admit(|| Ok("late transport")).is_err());
    }
}
