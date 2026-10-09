use anyerror::AnyError;
use openraft_macros::add_async_trait;

use crate::raft_state::LogIOId;
use crate::storage::LogFlushed;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::StorageIOError;
use crate::Vote;

/// Extension trait for RaftLogStorage to provide utility methods.
///
/// All methods in this trait are provided with default implementation.
#[add_async_trait]
pub trait RaftLogStorageExt<C>: RaftLogStorage<C>
where C: RaftTypeConfig
{
    /// Save a vote and wait for its durable completion before advancing Raft state.
    async fn blocking_save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let (tx, rx) = C::oneshot();
        let callback = LogFlushed::<C>::new(LogIOId::new(vote.clone(), None), tx);
        self.save_vote(vote, callback).await?;
        rx.await
            .map_err(|e| StorageIOError::write_vote(AnyError::error(e)))?
            .map_err(|e| StorageIOError::write_vote(AnyError::error(e)))?;
        Ok(())
    }

    /// Blocking mode append log entries to the storage.
    ///
    /// It blocks until the callback is called by the underlying storage implementation.
    async fn blocking_append<I>(&mut self, entries: I) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (tx, rx) = C::oneshot();

        // dummy log_io_id
        let log_io_id = LogIOId::<C::NodeId>::new(Vote::<C::NodeId>::default(), None);

        let callback = LogFlushed::<C>::new(log_io_id, tx);
        self.append(entries, callback).await?;
        rx.await
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?;

        Ok(())
    }
}

impl<C, T> RaftLogStorageExt<C> for T
where
    T: RaftLogStorage<C>,
    C: RaftTypeConfig,
{
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::io::Cursor;
    use std::ops::RangeBounds;

    use tokio::sync::mpsc;

    use super::*;
    use crate::Entry;
    use crate::LogId;
    use crate::LogState;
    use crate::RaftLogReader;

    crate::declare_raft_types!(TestConfig);

    #[derive(Clone)]
    struct VoteStore {
        callbacks: mpsc::UnboundedSender<LogFlushed<TestConfig>>,
        reject: bool,
    }

    #[cfg(not(feature = "storage-v2"))]
    impl crate::storage::v2::sealed::Sealed for VoteStore {}

    impl RaftLogReader<TestConfig> for VoteStore {
        async fn try_get_log_entries<RB>(&mut self, _range: RB) -> Result<Vec<Entry<TestConfig>>, StorageError<u64>>
        where RB: RangeBounds<u64> + Clone + Debug + OptionalSend {
            Ok(Vec::new())
        }
    }

    impl RaftLogStorage<TestConfig> for VoteStore {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<TestConfig>, StorageError<u64>> {
            Ok(LogState {
                last_purged_log_id: None,
                last_log_id: None,
            })
        }

        async fn get_log_reader(&mut self) -> Self {
            self.clone()
        }

        async fn save_vote(
            &mut self,
            _vote: &Vote<u64>,
            callback: LogFlushed<TestConfig>,
        ) -> Result<(), StorageError<u64>> {
            if self.reject {
                return Err(StorageIOError::write_vote(AnyError::error("submission failed")).into());
            }
            assert!(self.callbacks.send(callback).is_ok());
            Ok(())
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
            Ok(None)
        }

        async fn append<I>(&mut self, _entries: I, callback: LogFlushed<TestConfig>) -> Result<(), StorageError<u64>>
        where
            I: IntoIterator<Item = Entry<TestConfig>> + OptionalSend,
            I::IntoIter: OptionalSend,
        {
            callback.log_io_completed(Ok(()));
            Ok(())
        }

        async fn truncate(&mut self, _log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
            Ok(())
        }
        async fn purge(&mut self, _log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
            Ok(())
        }
    }

    fn vote_store() -> (VoteStore, mpsc::UnboundedReceiver<LogFlushed<TestConfig>>) {
        let (callbacks, rx) = mpsc::unbounded_channel();
        (
            VoteStore {
                callbacks,
                reject: false,
            },
            rx,
        )
    }

    #[tokio::test]
    async fn vote_waits_for_durable_completion() {
        let (mut store, mut callbacks) = vote_store();
        let vote = Vote::new(3, 1);
        let mut saving = Box::pin(store.blocking_save_vote(&vote));
        assert!(futures::poll!(saving.as_mut()).is_pending());
        let callback = callbacks.try_recv().expect("vote submitted");
        assert!(futures::poll!(saving.as_mut()).is_pending());
        callback.log_io_completed(Ok(()));
        saving.await.unwrap();
    }

    #[tokio::test]
    async fn vote_flush_failure_is_propagated() {
        let (mut store, mut callbacks) = vote_store();
        let vote = Vote::new(3, 1);
        let mut saving = Box::pin(store.blocking_save_vote(&vote));
        assert!(futures::poll!(saving.as_mut()).is_pending());
        callbacks.try_recv().unwrap().log_io_completed(Err(std::io::Error::other("flush failed")));
        let error = saving.await.unwrap_err().to_string();
        assert!(error.contains("Vote"));
        assert!(error.contains("flush failed"));
    }

    #[tokio::test]
    async fn missing_vote_completion_is_an_error() {
        let (mut store, mut callbacks) = vote_store();
        let vote = Vote::new(3, 1);
        let mut saving = Box::pin(store.blocking_save_vote(&vote));
        assert!(futures::poll!(saving.as_mut()).is_pending());
        drop(callbacks.try_recv().unwrap());
        assert!(saving.await.unwrap_err().to_string().contains("Vote"));
    }

    #[tokio::test]
    async fn vote_submission_failure_does_not_wait_for_a_callback() {
        let (mut store, _callbacks) = vote_store();
        store.reject = true;
        let error = store.blocking_save_vote(&Vote::new(3, 1)).await.unwrap_err();
        assert!(error.to_string().contains("submission failed"));
    }
}
