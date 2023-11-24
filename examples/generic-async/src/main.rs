use diesel::migration::{Migration, MigrationSource};
use diesel::pg::{Pg, PgConnection};
use diesel::prelude::*;
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
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

async fn run_migrations(db_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let async_connection = AsyncPgConnection::establish(db_url).await?;

    let mut async_wrapper: AsyncConnectionWrapper<AsyncPgConnection> =
        AsyncConnectionWrapper::from(async_connection);

    MIGRATIONS
        .migrations()
        .unwrap()
        .iter()
        .for_each(|m: &Box<dyn Migration<Pg>>| println!("{}", m.name()));
    tokio::task::spawn_blocking(move || {
        async_wrapper.run_pending_migrations(MIGRATIONS).unwrap();
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
    let mut sync_wrapper = SyncConnectionWrapper::<PgConnection>::establish(&db_url).await?;

    sync_wrapper.batch_execute("DELETE FROM users").await?;

    sync_wrapper
        .batch_execute("INSERT INTO users(id, name) VALUES (3, 'toto')")
        .await?;

    diesel::delete(users::table)
        .execute(&mut sync_wrapper)
        .await?;

    // diesel::insert_into(users::table)
    //     .values((users::id.eq(1), users::name.eq("iLuke")))
    //     .execute(&mut sync_wrapper)
    //     .await?;

    // // use ordinary diesel query dsl to construct your query
    // let data: Vec<User> = users::table
    //     .filter(users::id.gt(0))
    //     .or_filter(users::name.like("%Luke"))
    //     .select(User::as_select())
    //     // execute the query via the provided
    //     // async `diesel_async::RunQueryDsl`
    //     .load(&mut connection)
    //     .await?;
    //
    // println!("{data:?}");

    Ok(())
}
