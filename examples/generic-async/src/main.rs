use diesel::backend::Backend;
use diesel::migration::{Migration, MigrationConnection, MigrationSource};
#[cfg(feature = "postgres")]
use diesel::pg::{Pg, PgConnection};
use diesel::prelude::*;
#[cfg(feature = "sqlite")]
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

// ordinary diesel model setup

table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = users)]
struct User {
    id: i32,
    name: String,
}

const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

#[cfg(feature = "sqlite")]
type InnerConnection = SqliteConnection;
#[cfg(feature = "postgres")]
type InnerConnection = PgConnection;

#[cfg(feature = "sqlite")]
type InnerDB = Sqlite;
#[cfg(feature = "postgres")]
type InnerDB = Pg;

async fn establish(db_url: &str) -> ConnectionResult<SyncConnectionWrapper<InnerConnection>> {
    #[cfg(feature = "sqlite")]
    {
        SyncConnectionWrapper::<SqliteConnection>::establish(db_url).await
    }
    #[cfg(feature = "postgres")]
    {
        SyncConnectionWrapper::<PgConnection>::establish(db_url).await
    }
}

#[cfg(feature = "sqlite")]
fn establish_inner_conn(db_url: &str) -> InnerConnection {
    SqliteConnection::establish(db_url).unwrap()
}

#[cfg(feature = "postgres")]
fn establish_inner_conn(db_url: &str) -> InnerConnection {
    PgConnection::establish(db_url).unwrap()
}

async fn run_migrations(db_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut sync_conn = establish_inner_conn(db_url);

    // run_migrations_for(sync_conn).await
    tokio::task::spawn_blocking(move || {
        sync_conn.run_pending_migrations(MIGRATIONS).unwrap();
    })
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Should be in the form of postgres://user:password@localhost/database?sslmode=require
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");
    run_migrations(&db_url).await?;

    // create an async connection
    let mut sync_wrapper: SyncConnectionWrapper<InnerConnection> = establish(&db_url).await?;

    sync_wrapper.batch_execute("DELETE FROM users").await?;

    sync_wrapper
        .batch_execute("INSERT INTO users(id, name) VALUES (3, 'toto')")
        .await?;

    diesel::delete(users::table)
        .execute(&mut sync_wrapper)
        .await?;

    diesel::insert_into(users::table)
        .values((users::id.eq(1), users::name.eq("iLuke")))
        .execute(&mut sync_wrapper)
        .await?;

    // // use ordinary diesel query dsl to construct your query
    // let data: Vec<User> = users::table
    //     .filter(users::id.gt(0))
    //     .or_filter(users::name.like("%Luke"))
    //     .select(User::as_select())
    //     // execute the query via the provided
    //     // async `diesel_async::RunQueryDsl`
    //     .load(&mut sync_wrapper)
    //     .await?;
    // println!("{data:?}");

    Ok(())
}
