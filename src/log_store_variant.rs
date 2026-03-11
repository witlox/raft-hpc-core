//! Polymorphic log store that switches between in-memory and file-backed storage.

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogId, LogState, OptionalSend, RaftLogReader, RaftTypeConfig};

use crate::persistent_store::{FileLogReader, FileLogStore};
use crate::store::{MemLogReader, MemLogStore};

/// A log store that can be either in-memory or file-backed.
#[derive(Clone)]
pub enum LogStoreVariant<C: RaftTypeConfig> {
    Memory(MemLogStore<C>),
    File(FileLogStore<C>),
}

/// A log reader that matches the `LogStoreVariant`.
#[derive(Clone)]
pub enum LogReaderVariant<C: RaftTypeConfig> {
    Memory(MemLogReader<C>),
    File(FileLogReader<C>),
}

impl<C> RaftLogReader<C> for LogReaderVariant<C>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
    >,
    openraft::Entry<C>: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        match self {
            Self::Memory(r) => r.try_get_log_entries(range).await,
            Self::File(r) => r.try_get_log_entries(range).await,
        }
    }

    async fn read_vote(&mut self) -> Result<Option<C::Vote>, io::Error> {
        match self {
            Self::Memory(r) => r.read_vote().await,
            Self::File(r) => r.read_vote().await,
        }
    }
}

impl<C> RaftLogStorage<C> for LogStoreVariant<C>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
    >,
    openraft::Entry<C>: Clone,
{
    type LogReader = LogReaderVariant<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        match self {
            Self::Memory(s) => s.get_log_state().await,
            Self::File(s) => s.get_log_state().await,
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        match self {
            Self::Memory(s) => LogReaderVariant::Memory(s.get_log_reader().await),
            Self::File(s) => LogReaderVariant::File(s.get_log_reader().await),
        }
    }

    async fn save_vote(&mut self, vote: &C::Vote) -> Result<(), io::Error> {
        match self {
            Self::Memory(s) => s.save_vote(vote).await,
            Self::File(s) => s.save_vote(vote).await,
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<C>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        match self {
            Self::Memory(s) => s.append(entries, callback).await,
            Self::File(s) => s.append(entries, callback).await,
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId<C>>) -> Result<(), io::Error> {
        match self {
            Self::Memory(s) => s.truncate_after(last_log_id).await,
            Self::File(s) => s.truncate_after(last_log_id).await,
        }
    }

    async fn purge(&mut self, log_id: LogId<C>) -> Result<(), io::Error> {
        match self {
            Self::Memory(s) => s.purge(log_id).await,
            Self::File(s) => s.purge(log_id).await,
        }
    }

    async fn save_committed(&mut self, committed: Option<LogId<C>>) -> Result<(), io::Error> {
        match self {
            Self::Memory(s) => s.save_committed(committed).await,
            Self::File(s) => s.save_committed(committed).await,
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C>>, io::Error> {
        match self {
            Self::Memory(s) => s.read_committed().await,
            Self::File(s) => s.read_committed().await,
        }
    }
}
