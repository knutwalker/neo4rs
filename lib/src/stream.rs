use crate::{
    bolt::{Discard, Pull, Response, Streaming, StreamingSummary, Summary, WrapExtra as _},
    errors::{Error, Result},
    pool::ManagedConnection,
    row::Row,
    txn::TransactionHandle,
    types::BoltList,
    DeError,
};
use futures::{stream::try_unfold, TryStream};
use serde::de::DeserializeOwned;
use std::collections::VecDeque;

/// An abstraction over a stream of rows, this is returned as a result of [`crate::Txn::execute`].
///
/// A stream needs a running transaction to be consumed.
#[must_use = "Results must be streamed through with `next` in order to execute the query"]
pub struct RowStream {
    qid: i64,
    fields: BoltList,
    state: State,
    fetch_size: usize,
    buffer: VecDeque<Row>,
}

/// An abstraction over a stream of rows, this is returned as a result of [`crate::Graph::execute`].
///
/// A stream will contain a connection from the connection pool which will be released to the pool
/// when the stream is dropped.
#[must_use = "Results must be streamed through with `next` in order to execute the query"]
pub struct DetachedRowStream {
    stream: RowStream,
    connection: ManagedConnection,
}

impl RowStream {
    pub(crate) fn new(qid: i64, fields: BoltList, fetch_size: usize) -> Self {
        RowStream {
            qid,
            fields,
            fetch_size,
            state: State::Ready,
            buffer: VecDeque::with_capacity(fetch_size),
        }
    }
}

impl DetachedRowStream {
    pub(crate) fn new(stream: RowStream, connection: ManagedConnection) -> Self {
        DetachedRowStream { stream, connection }
    }
}

impl RowStream {
    pub async fn finish(
        mut self,
        mut handle: impl TransactionHandle,
    ) -> Result<Option<StreamingSummary>> {
        loop {
            if let State::Complete(s) = self.state {
                return Ok(s.map(|o| *o));
            }
            let summary = {
                let connected = handle.connection();
                connected
                    .send_recv_as(Discard::all().for_query(self.qid))
                    .await
            }?;
            match summary {
                Summary::Success(s) => match s.metadata {
                    Streaming::Done(summary) => self.state = State::Complete(Some(summary)),
                    Streaming::HasMore => {}
                },
                Summary::Ignored => self.state = State::Complete(None),
                Summary::Failure(f) => {
                    self.state = State::Complete(None);
                    return Err(Error::Failure {
                        code: f.code,
                        message: f.message,
                        msg: "DISCARD",
                    });
                }
            }
        }
    }

    /// A call to next() will return a row from an internal buffer if the buffer has any entries,
    /// if the buffer is empty and the server has more rows left to consume, then a new batch of rows
    /// are fetched from the server (using the fetch_size value configured see [`crate::ConfigBuilder::fetch_size`])
    pub async fn next(&mut self, mut handle: impl TransactionHandle) -> Result<Option<Row>> {
        loop {
            match &self.state {
                State::Ready => {
                    let pull = Pull::some(self.fetch_size as i64).for_query(self.qid);
                    let connection = handle.connection();
                    connection.send_as(pull).await?;
                    self.state = State::Streaming;
                }
                State::Streaming => {
                    let connection = handle.connection();
                    let response = connection
                        .recv_as::<Response<BoltList, Streaming>>()
                        .await?;
                    match response {
                        Response::Detail(record) => {
                            let row = Row::new(self.fields.clone(), record);
                            self.buffer.push_back(row);
                        }
                        Response::Success(Streaming::HasMore) => self.state = State::Buffered,
                        Response::Success(Streaming::Done(s)) => {
                            self.state = State::Complete(Some(s))
                        }
                        otherwise => return Err(otherwise.into_error("PULL")),
                    }
                }
                State::Buffered => {
                    if !self.buffer.is_empty() {
                        return Ok(self.buffer.pop_front());
                    }
                    self.state = State::Ready;
                }
                State::Complete(_) => {
                    return Ok(self.buffer.pop_front());
                }
            }
        }
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every element is a [`crate::row::Row`].
    pub fn into_stream(
        self,
        handle: impl TransactionHandle,
    ) -> impl TryStream<Ok = Row, Error = Error> {
        try_unfold((self, handle), |(mut stream, mut hd)| async move {
            match stream.next(&mut hd).await {
                Ok(Some(row)) => Ok(Some((row, (stream, hd)))),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every row is converted into a `T` by calling [`crate::row::Row::to`].
    pub fn into_stream_as<T: DeserializeOwned>(
        self,
        handle: impl TransactionHandle,
    ) -> impl TryStream<Ok = T, Error = Error> {
        self.into_stream_de(handle, |row| row.to::<T>())
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// the value at the given column is converted into a `T`
    /// by calling [`crate::row::Row::get`].
    pub fn column_into_stream<'db, T: DeserializeOwned + 'db>(
        self,
        handle: impl TransactionHandle + 'db,
        column: &'db str,
    ) -> impl TryStream<Ok = T, Error = Error> + 'db {
        self.into_stream_de(handle, move |row| row.get::<T>(column))
    }

    fn into_stream_de<T: DeserializeOwned>(
        self,
        handle: impl TransactionHandle,
        deser: impl Fn(Row) -> Result<T, DeError>,
    ) -> impl TryStream<Ok = T, Error = Error> {
        try_unfold(
            (self, handle, deser),
            |(mut stream, mut hd, de)| async move {
                match stream.next(&mut hd).await {
                    Ok(Some(row)) => match de(row) {
                        Ok(res) => Ok(Some((res, (stream, hd, de)))),
                        Err(e) => Err(Error::DeserializationError(e)),
                    },
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                }
            },
        )
    }
}

impl DetachedRowStream {
    /// A call to next() will return a row from an internal buffer if the buffer has any entries,
    /// if the buffer is empty and the server has more rows left to consume, then a new batch of rows are fetched from the server (using the
    /// fetch_size value configured see [`crate::ConfigBuilder::fetch_size`])
    pub async fn next(&mut self) -> Result<Option<Row>> {
        self.stream.next(&mut self.connection).await
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every element is a [`crate::row::Row`].
    pub fn into_stream(self) -> impl TryStream<Ok = Row, Error = Error> {
        self.stream.into_stream(self.connection)
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every row is converted into a `T` by calling [`crate::row::Row::to`].
    /// If the conversion fails and the result stream has exactly one column,
    /// that single value will be converted by calling [`crate::BoltType::to`].
    pub fn into_stream_as<T: DeserializeOwned>(self) -> impl TryStream<Ok = T, Error = Error> {
        self.stream.into_stream_as(self.connection)
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// the value at the given column is converted into a `T`
    /// by calling [`crate::row::Row::get`].
    pub fn column_into_stream<'db, T: DeserializeOwned + 'db>(
        self,
        column: &'db str,
    ) -> impl TryStream<Ok = T, Error = Error> + 'db {
        self.stream.column_into_stream(self.connection, column)
    }
}

#[derive(Clone, PartialEq, Debug)]
enum State {
    Ready,
    Streaming,
    Buffered,
    Complete(Option<Box<StreamingSummary>>),
}
