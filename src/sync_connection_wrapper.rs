use crate::{AnsiTransactionManager, AsyncConnection, SimpleAsyncConnection};
use diesel::backend::Backend;
use diesel::connection::LoadConnection;
use diesel::query_builder::{
    AsQuery, BindCollector, CollectedQuery, MovableBindCollector, QueryBuilder, QueryFragment,
    QueryId,
};
use diesel::sql_types::TypeMetadata;
use diesel::{Connection, ConnectionResult, QueryResult};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::FutureExt;
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

trait WithMetadataLookup: Connection {
    fn metadata_lookup(&mut self) -> &mut <Self::Backend as TypeMetadata>::MetadataLookup;
}

#[cfg(feature = "sqlite")]
static mut SQLITE_METADATA_LOOKUP: () = ();

#[cfg(feature = "sqlite")]
impl<C> WithMetadataLookup for C
where
    C: diesel::connection::Connection<Backend = diesel::sqlite::Sqlite>,
{
    fn metadata_lookup(&mut self) -> &mut <Self::Backend as TypeMetadata>::MetadataLookup {
        // safe since it's unit type
        unsafe { &mut SQLITE_METADATA_LOOKUP }
    }
}

#[cfg(feature = "postgres")]
impl<C> WithMetadataLookup for C
where
    C: diesel::connection::Connection<Backend = diesel::pg::Pg>
        + diesel::pg::PgMetadataLookup
        + 'static,
{
    fn metadata_lookup(&mut self) -> &mut <Self::Backend as TypeMetadata>::MetadataLookup {
        self
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

use diesel::query_builder::bind_collector::SendableCollector;

#[async_trait::async_trait]
impl<C, MD> AsyncConnection for SyncConnectionWrapper<C>
where
    C: diesel::connection::Connection
        + diesel::connection::LoadConnection
        + WithMetadataLookup
        + 'static,
    <C as Connection>::Backend:
        std::default::Default + diesel::backend::DieselReserveSpecialization,
    <C::Backend as Backend>::QueryBuilder: std::default::Default,
    MD: SendableCollector,
    for<'a> <C::Backend as Backend>::BindCollector<'a>:
        MovableBindCollector<C::Backend, MovableData = MD> + std::default::Default,
{
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type Stream<'conn, 'query> = BoxStream<
        'static,
        QueryResult<<C as diesel::connection::LoadConnection>::Row<'conn, 'query>>,
    >;
    type Row<'conn, 'query> = <C as diesel::connection::LoadConnection>::Row<'conn, 'query>;
    type Backend = <C as Connection>::Backend;
    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let database_url = database_url.to_string();
        tokio::task::spawn_blocking(move || C::establish(&database_url))
            .await
            .unwrap_or_else(|e| Err(diesel::ConnectionError::BadConnection(e.to_string())))
            .map(|c| SyncConnectionWrapper::new(c))
    }

    fn load<'conn, 'query, T>(&'conn mut self, _source: T) -> Self::LoadFuture<'conn, 'query>
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
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let backend = <Self as AsyncConnection>::Backend::default();

        let (collect_bind_result, movable_collector) = {
            let exclusive = self.inner.clone();
            let mut inner = exclusive.lock().unwrap();
            let mut bind_collector = <<<Self as AsyncConnection>::Backend as Backend>::BindCollector<
                '_,
            > as Default>::default();
            let mut metadata_lookup = inner.metadata_lookup();
            // value in source could be stored in bind_collector
            // source must live at least as long as bind collector
            let result = source.collect_binds(&mut bind_collector, metadata_lookup, &backend);
            let movable_collector = bind_collector.movable();

            let mut query_builder =
                <<<Self as AsyncConnection>::Backend as Backend>::QueryBuilder as Default>::default(
                );
            let sql = source
                .to_sql(&mut query_builder, &backend)
                .map(|_| query_builder.finish());
            println!("Generated SQL '{:?}'", sql);

            (result, movable_collector)
        };

        let mut query_builder =
            <<<Self as AsyncConnection>::Backend as Backend>::QueryBuilder as Default>::default();
        let sql = source
            .to_sql(&mut query_builder, &backend)
            .map(|_| query_builder.finish());
        println!("Generated SQL '{:?}'", sql);
        let is_safe_to_cache_prepared = source.is_safe_to_cache_prepared(&backend);

        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            collect_bind_result?;
            let query = CollectedQuery::new(sql?, is_safe_to_cache_prepared?, movable_collector);
            let mut inner = inner.lock().unwrap();
            inner.execute_returning_count(&query)
            // Ok(0)
        })
        .map(|fut| fut.unwrap_or_else(|e| Err(from_tokio_join_error(e))))
        .boxed()
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        unimplemented!()
    }
}
