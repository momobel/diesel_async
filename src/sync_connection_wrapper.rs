use crate::{AnsiTransactionManager, AsyncConnection, SimpleAsyncConnection};
use diesel::query_builder::{AsQuery, QueryBuilder, QueryFragment, QueryId};
use diesel::{ConnectionError, ConnectionResult, QueryResult};
use futures_util::future::BoxFuture;
use futures_util::stream::{BoxStream, TryStreamExt};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task::JoinError;

fn from_tokio_join_error(join_error: JoinError) -> diesel::result::Error {
    diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::UnableToSendCommand,
        Box::new(join_error.to_string()),
    )
}

pub struct SyncConnectionWrapper<C> {
    inner: Arc<Mutex<C>>,
}

impl<C> SyncConnectionWrapper<C> {
    pub fn new(connection: C) -> Self {
        SyncConnectionWrapper {
            inner: Arc::new(Mutex::new(connection)),
        }
    }
}

#[async_trait::async_trait]
impl<C> SimpleAsyncConnection for SyncConnectionWrapper<C>
where
    C: diesel::connection::Connection + 'static,
{
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        // JoinHandle has Output=Future<Result<QueryResult, JoinError>>
        // QueryResult = Diesel::Result<(), Diesel::Error>
        // ? -> QueryResult
        // transform JoinError to Diesel::Error
        let query = query.to_string();
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.lock().unwrap().batch_execute(query.as_str()))
            .await
            .unwrap_or_else(|e| Err(from_tokio_join_error(e)))
    }
}

#[async_trait::async_trait]
impl<C> AsyncConnection for SyncConnectionWrapper<C>
where
    C: diesel::connection::Connection + diesel::connection::LoadConnection + 'static,
{
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type Stream<'conn, 'query> = BoxStream<
        'static,
        QueryResult<<C as diesel::connection::LoadConnection>::Row<'conn, 'query>>,
    >;
    type Row<'conn, 'query> = <C as diesel::connection::LoadConnection>::Row<'conn, 'query>;
    type Backend = C::Backend;
    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let database_url = database_url.to_string();
        tokio::task::spawn_blocking(move || C::establish(&database_url))
            .await
            .unwrap_or_else(|e| Err(diesel::ConnectionError::BadConnection(e.to_string())))
            .map(|c| SyncConnectionWrapper::new(c))
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        unimplemented!()
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        unimplemented!()
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        unimplemented!()
    }
}
