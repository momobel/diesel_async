use crate::{AnsiTransactionManager, AsyncConnection, SimpleAsyncConnection};
use diesel::backend::Backend;
use diesel::query_builder::{AsQuery, QueryBuilder, QueryFragment, QueryId};
use diesel::{Connection, ConnectionError, ConnectionResult, QueryResult};
use futures_util::future::BoxFuture;
use futures_util::stream::{BoxStream, TryStreamExt};
use futures_util::{Future, FutureExt, StreamExt};
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

impl<C: diesel::connection::Connection> SyncConnectionWrapper<C> {
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
    C: diesel::Connection<Backend = diesel::pg::Pg>,
    <C as diesel::Connection>::Backend: std::default::Default,
    <<C as diesel::Connection>::Backend as Backend>::QueryBuilder: std::default::Default,
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
        // we explicilty descruct the query here before going into the async block
        //
        // That's required to remove the send bound from `T` as we have translated
        // the query type to just a string (for the SQL) and a bunch of bytes (for the binds)
        // which both are `Send`.
        // We also collect the query id (essentially an integer) and the safe_to_cache flag here
        // so there is no need to even access the query in the async block below
        let mut query_builder =
            <<<Self as AsyncConnection>::Backend as Backend>::QueryBuilder as Default>::default();
        let sql = source
            .to_sql(
                &mut query_builder,
                &<Self as AsyncConnection>::Backend::default(),
            )
            .map(|_| query_builder.finish());
        println!("{:?}", sql);

        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let sql = diesel::sql_query(sql?);
            inner.lock().unwrap().execute_returning_count(&sql)
        })
        .map(|fut| fut.unwrap_or_else(|e| Err(from_tokio_join_error(e))))
        .boxed()
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        unimplemented!()
    }
}
