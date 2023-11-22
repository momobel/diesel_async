use crate::SimpleAsyncConnection;
use diesel::QueryResult;
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
