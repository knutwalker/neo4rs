use crate::{
    bolt::{Bolt, Discard, Pull, Response, Streaming, StreamingSummary, Summary, WrapExtra as _},
    errors::{Error, Result},
    pool::ManagedConnection,
    row::Row,
    txn::TransactionHandle,
    types::BoltList,
    BoltType, DeError,
};
use futures::{stream::try_unfold, FutureExt as _, TryStream, TryStreamExt as _};
use serde::de::DeserializeOwned;
use std::{collections::VecDeque, future::Future};

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

pub enum RowItem<T = Row> {
    Row(T),
    Summary(Box<StreamingSummary>),
    Done,
}

impl<T> RowItem<T> {
    pub fn row(&self) -> Option<&T> {
        match self {
            RowItem::Row(row) => Some(row),
            _ => None,
        }
    }

    pub fn summary(&self) -> Option<&StreamingSummary> {
        match self {
            RowItem::Summary(summary) => Some(summary),
            _ => None,
        }
    }

    pub fn into_row(self) -> Option<T> {
        match self {
            RowItem::Row(row) => Some(row),
            _ => None,
        }
    }

    pub fn into_summary(self) -> Option<Box<StreamingSummary>> {
        match self {
            RowItem::Summary(summary) => Some(summary),
            _ => None,
        }
    }
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
    /// A call to next() will return a row from an internal buffer if the buffer has any entries,
    /// if the buffer is empty and the server has more rows left to consume, then a new batch of rows
    /// are fetched from the server (using the fetch_size value configured see [`crate::ConfigBuilder::fetch_size`])
    pub async fn next(&mut self, handle: impl TransactionHandle) -> Result<Option<Row>> {
        self.next_or_summary(handle)
            .await
            .map(|item| item.into_row())
    }

    /// A call to next_or_summary() will return a row from an internal buffer if the buffer has any entries,
    /// if the buffer is empty and the server has more rows left to consume, then a new batch of rows
    /// are fetched from the server (using the fetch_size value configured see [`crate::ConfigBuilder::fetch_size`])
    pub async fn next_or_summary(&mut self, mut handle: impl TransactionHandle) -> Result<RowItem> {
        loop {
            if let Some(row) = self.buffer.pop_front() {
                break Ok(RowItem::Row(row));
            }

            match self.state {
                State::Ready => {
                    let pull = Pull::some(self.fetch_size as i64).for_query(self.qid);
                    let connection = handle.connection();
                    connection.send_as(pull).await?;
                    self.state = State::Pulling;
                }
                State::Pulling => {
                    let connection = handle.connection();
                    let response = connection
                        .recv_as::<Response<Vec<Bolt>, Streaming>>()
                        .await?;
                    match response {
                        Response::Detail(record) => {
                            let record = BoltList::from(
                                record
                                    .into_iter()
                                    .map(BoltType::from)
                                    .collect::<Vec<BoltType>>(),
                            );
                            let row = Row::new(self.fields.clone(), record);
                            self.buffer.push_back(row);
                        }
                        Response::Success(Streaming::HasMore) => self.state = State::Ready,
                        Response::Success(Streaming::Done(s)) => {
                            self.state = State::Complete(Some(s))
                        }
                        otherwise => return Err(otherwise.into_error("PULL")),
                    }
                }
                State::Complete(ref mut summary) => {
                    break match summary.take() {
                        Some(summary) => Ok(RowItem::Summary(summary)),
                        None => Ok(RowItem::Done),
                    };
                }
            }
        }
    }

    /// Stop consuming the stream adn return a summary, if available.
    /// Stopping the stream will also discard any messages on the server side.
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

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every element is a [`crate::row::Row`].
    pub fn into_stream(
        self,
        handle: impl TransactionHandle,
    ) -> impl TryStream<Ok = Row, Error = Error> {
        self.into_stream_convert(handle, Ok)
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every row is converted into a `T` by calling [`crate::row::Row::to`].
    pub fn into_stream_as<T: DeserializeOwned>(
        self,
        handle: impl TransactionHandle,
    ) -> impl TryStream<Ok = T, Error = Error> {
        self.into_stream_convert(handle, |row| row.to::<T>())
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// the value at the given column is converted into a `T`
    /// by calling [`crate::row::Row::get`].
    pub fn column_into_stream<'db, T: DeserializeOwned + 'db>(
        self,
        handle: impl TransactionHandle + 'db,
        column: &'db str,
    ) -> impl TryStream<Ok = T, Error = Error> + 'db {
        self.into_stream_convert(handle, move |row| row.get::<T>(column))
    }

    /// Turns this RowStream into a pair of
    ///     1. a [`futures::stream::TryStream`] where every element is a [`crate::row::Row`]
    ///     2. a [`futures::Future`] that resolves to the summary when the stream is done
    pub fn split_into_stream_and_summary(
        self,
        handle: impl TransactionHandle,
    ) -> (
        impl TryStream<Ok = Row, Error = Error>,
        impl Future<Output = Option<StreamingSummary>>,
    ) {
        self.split_into_stream_convert(handle, Ok)
    }

    /// Turns this RowStream into a pair of
    ///     1. a [`futures::stream::TryStream`] where every element is
    ///         converted into a `T` by calling [`crate::row::Row::to`]
    ///     2. a [`futures::Future`] that resolves to the summary when the stream is done
    pub fn split_into_stream_and_summary_as<T: DeserializeOwned>(
        self,
        handle: impl TransactionHandle,
    ) -> (
        impl TryStream<Ok = T, Error = Error>,
        impl Future<Output = Option<StreamingSummary>>,
    ) {
        self.split_into_stream_convert(handle, |row| row.to::<T>())
    }

    /// Turns this RowStream into a pair of
    ///     1. a [`futures::stream::TryStream`] where every element is
    ///         converted into a `T` by calling [`crate::row::Row::get`]
    ///     2. a [`futures::Future`] that resolves to the summary when the stream is done
    pub fn column_split_into_stream<'db, T: DeserializeOwned + 'db>(
        self,
        handle: impl TransactionHandle + 'db,
        column: &'db str,
    ) -> (
        impl TryStream<Ok = T, Error = Error> + 'db,
        impl Future<Output = Option<StreamingSummary>> + 'db,
    ) {
        self.split_into_stream_convert(handle, move |row| row.get::<T>(column))
    }

    /// Consume RowStream as a [`futures::stream::TryStream`] where
    /// every element is a [`crate::row::Row`].
    /// The stream can only be converted once.
    /// After the returned stream is consumed, this stream can be [`Self::finish`]ed to get the summary.
    pub fn mut_stream<'this, 'db: 'this>(
        &'this mut self,
        handle: impl TransactionHandle + 'db,
    ) -> impl TryStream<Ok = Row, Error = Error> + 'this {
        self.convert_stream_mut(handle, Ok)
    }

    /// Consume RowStream as a [`futures::stream::TryStream`] where
    /// every element is converted into a `T` by calling [`crate::row::Row::to`].
    /// The stream can only be converted once.
    /// After the returned stream is consumed, this stream can be [`Self::finish`]ed to get the summary.
    pub fn mut_stream_as<'this, 'db: 'this, T: DeserializeOwned>(
        &'this mut self,
        handle: impl TransactionHandle + 'db,
    ) -> impl TryStream<Ok = T, Error = Error> + 'this {
        self.convert_stream_mut(handle, |row| row.to::<T>())
    }

    /// Consume RowStream as a [`futures::stream::TryStream`] where
    /// every element is converted into a `T` by calling [`crate::row::Row::get`].
    /// The stream can only be converted once.
    /// After the returned stream is consumed, this stream can be [`Self::finish`]ed to get the summary.
    pub fn mut_column_stream<'this, 'db: 'this, T: DeserializeOwned>(
        &'this mut self,
        handle: impl TransactionHandle + 'db,
        column: &'db str,
    ) -> impl TryStream<Ok = T, Error = Error> + 'this {
        self.convert_stream_mut(handle, move |row| row.get::<T>(column))
    }

    fn into_stream_convert<T>(
        self,
        handle: impl TransactionHandle,
        convert: impl Fn(Row) -> Result<T, DeError>,
    ) -> impl TryStream<Ok = T, Error = Error> {
        self.into_stream_convert_and_summary(handle, convert)
            .try_filter_map(|row| std::future::ready(Ok(row.into_row())))
    }

    fn split_into_stream_convert<T>(
        self,
        handle: impl TransactionHandle,
        convert: impl Fn(Row) -> Result<T, DeError>,
    ) -> (
        impl TryStream<Ok = T, Error = Error>,
        impl Future<Output = Option<StreamingSummary>>,
    ) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut sender = Some(sender);
        (
            self.into_stream_convert_and_summary(handle, convert)
                .try_filter_map(move |row| {
                    std::future::ready(match row {
                        RowItem::Row(row) => Ok(Some(row)),
                        RowItem::Summary(summary) => {
                            if let Some(sender) = sender.take() {
                                let _ = sender.send(Some(*summary));
                            }
                            Ok(None)
                        }
                        RowItem::Done => {
                            if let Some(sender) = sender.take() {
                                let _ = sender.send(None);
                            }
                            Ok(None)
                        }
                    })
                }),
            receiver.map(|item| item.ok().flatten()),
        )
    }

    fn into_stream_convert_and_summary<T>(
        self,
        handle: impl TransactionHandle,
        convert: impl Fn(Row) -> Result<T, DeError>,
    ) -> impl TryStream<Ok = RowItem<T>, Error = Error> {
        try_unfold(
            (self, handle, convert),
            |(mut stream, mut hd, de)| async move {
                match stream.next_or_summary(&mut hd).await {
                    Ok(RowItem::Row(row)) => match de(row) {
                        Ok(res) => Ok(Some((RowItem::Row(res), (stream, hd, de)))),
                        Err(e) => Err(Error::DeserializationError(e)),
                    },
                    Ok(RowItem::Summary(summary)) => {
                        Ok(Some((RowItem::Summary(summary), (stream, hd, de))))
                    }
                    Ok(RowItem::Done) => Ok(None),
                    Err(e) => Err(e),
                }
            },
        )
    }

    fn convert_stream_mut<'this, 'db: 'this, T>(
        &'this mut self,
        handle: impl TransactionHandle + 'db,
        convert: impl Fn(Row) -> Result<T, DeError> + 'this,
    ) -> impl TryStream<Ok = T, Error = Error> + 'this {
        try_unfold((self, handle, convert), |(stream, mut hd, de)| async move {
            match stream.next(&mut hd).await {
                Ok(Some(row)) => match de(row) {
                    Ok(res) => Ok(Some((res, (stream, hd, de)))),
                    Err(e) => Err(Error::DeserializationError(e)),
                },
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }
}

impl DetachedRowStream {
    /// A call to next() will return a row from an internal buffer if the buffer has any entries,
    /// if the buffer is empty and the server has more rows left to consume, then a new batch of rows
    /// are fetched from the server (using the fetch_size value configured see [`crate::ConfigBuilder::fetch_size`])
    pub async fn next(&mut self) -> Result<Option<Row>> {
        self.stream.next(&mut self.connection).await
    }

    /// A call to next_or_summary() will return a row from an internal buffer if the buffer has any entries,
    /// if the buffer is empty and the server has more rows left to consume, then a new batch of rows
    /// are fetched from the server (using the fetch_size value configured see [`crate::ConfigBuilder::fetch_size`])
    pub async fn next_or_summary(&mut self) -> Result<RowItem> {
        self.stream.next_or_summary(&mut self.connection).await
    }

    /// Stop consuming the stream adn return a summary, if available.
    /// Stopping the stream will also discard any messages on the server side.
    pub async fn finish(self) -> Result<Option<StreamingSummary>> {
        self.stream.finish(self.connection).await
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every element is a [`crate::row::Row`].
    pub fn into_stream(self) -> impl TryStream<Ok = Row, Error = Error> {
        self.stream.into_stream(self.connection)
    }

    /// Turns this RowStream into a [`futures::stream::TryStream`] where
    /// every row is converted into a `T` by calling [`crate::row::Row::to`].
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

    /// Turns this RowStream into a pair of
    ///     1. a [`futures::stream::TryStream`] where every element is a [`crate::row::Row`]
    ///     2. a [`futures::Future`] that resolves to the summary when the stream is done
    pub fn split_into_stream_and_summary(
        self,
    ) -> (
        impl TryStream<Ok = Row, Error = Error>,
        impl Future<Output = Option<StreamingSummary>>,
    ) {
        self.stream.split_into_stream_and_summary(self.connection)
    }

    /// Turns this RowStream into a pair of
    ///     1. a [`futures::stream::TryStream`] where every element is
    ///         converted into a `T` by calling [`crate::row::Row::to`]
    ///     2. a [`futures::Future`] that resolves to the summary when the stream is done
    pub fn split_into_stream_and_summary_as<T: DeserializeOwned>(
        self,
    ) -> (
        impl TryStream<Ok = T, Error = Error>,
        impl Future<Output = Option<StreamingSummary>>,
    ) {
        self.stream
            .split_into_stream_and_summary_as(self.connection)
    }

    /// Turns this RowStream into a pair of
    ///     1. a [`futures::stream::TryStream`] where every element is
    ///         converted into a `T` by calling [`crate::row::Row::get`]
    ///     2. a [`futures::Future`] that resolves to the summary when the stream is done
    pub fn column_split_into_stream<'db, T: DeserializeOwned + 'db>(
        self,
        column: &'db str,
    ) -> (
        impl TryStream<Ok = T, Error = Error> + 'db,
        impl Future<Output = Option<StreamingSummary>> + 'db,
    ) {
        self.stream
            .column_split_into_stream(self.connection, column)
    }

    /// Consume RowStream as a [`futures::stream::TryStream`] where
    /// every element is a [`crate::row::Row`].
    /// The stream can only be converted once.
    /// After the returned stream is consumed, this stream can be [`Self::finish`]ed to get the summary.
    pub fn mut_stream(&mut self) -> impl TryStream<Ok = Row, Error = Error> + '_ {
        self.stream.mut_stream(&mut self.connection)
    }

    /// Consume RowStream as a [`futures::stream::TryStream`] where
    /// every element is converted into a `T` by calling [`crate::row::Row::to`].
    /// The stream can only be converted once.
    /// After the returned stream is consumed, this stream can be [`Self::finish`]ed to get the summary.
    pub fn mut_stream_as<T: DeserializeOwned>(
        &mut self,
    ) -> impl TryStream<Ok = T, Error = Error> + '_ {
        self.stream.mut_stream_as(&mut self.connection)
    }

    /// Consume RowStream as a [`futures::stream::TryStream`] where
    /// every element is converted into a `T` by calling [`crate::row::Row::get`].
    /// The stream can only be converted once.
    /// After the returned stream is consumed, this stream can be [`Self::finish`]ed to get the summary.
    pub fn mut_column_stream<'this, 'db: 'this, T: DeserializeOwned>(
        &'this mut self,
        column: &'db str,
    ) -> impl TryStream<Ok = T, Error = Error> + 'this {
        self.stream.mut_column_stream(&mut self.connection, column)
    }
}

#[derive(Clone, PartialEq, Debug)]
enum State {
    Ready,
    Pulling,
    Complete(Option<Box<StreamingSummary>>),
}
