#[macro_use]
extern crate log;

use actix_web::{delete, post, put, HttpResponse, ResponseError};
use actix_web::{web, App, HttpServer};

use actix_web::middleware::Logger;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use simplelog::{Config, TermLogger, TerminalMode};

use crate::config::load_db_config;
use crate::db::{DbError, DbManager};
use actix_web::web::Bytes;

mod config;
mod db;

type Response<T> = Result<T, DbError>;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Err {
    Msg(String),
}

#[derive(Deserialize)]
struct PathVal {
    db_name: String,
    key: String,
}

impl ResponseError for DbError {
    fn error_response(&self) -> HttpResponse {
        match self {
            DbError::Rocks(e) => HttpResponse::InternalServerError().json(Err::Msg(e.to_string())),
            DbError::Validation(s) => HttpResponse::BadRequest().json(Err::Msg(s.into())),
        }
    }
}

#[post("/{db_name}")]
async fn open(db_name: web::Path<String>, db_man: web::Data<DbManager>) -> Response<HttpResponse> {
    db_man.open(db_name.into_inner()).await?;
    Ok(HttpResponse::Ok().finish())
}

#[delete("/{db_name}")]
async fn close(db_name: web::Path<String>, db_man: web::Data<DbManager>) -> Response<HttpResponse> {
    db_man.close(db_name.into_inner()).await?;
    Ok(HttpResponse::Ok().finish())
}

#[put("/{db_name}/{key}")]
async fn store(
    p_val: web::Path<PathVal>,
    body: Bytes,
    db_man: web::Data<DbManager>,
) -> Response<HttpResponse> {
    db_man
        .store(p_val.db_name.as_str(), p_val.key.as_str(), body)
        .await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=debug");
    std::env::set_var("RUST_BACKTRACE", "1");
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap();

    let db_cfg = load_db_config().expect("Failed to start - can't load db config");
    info!("Db config = {:?}", db_cfg);

    let db_manager = DbManager::new(db_cfg).expect("handle err");
    db_manager.init().expect("handle err");
    let db_manager = web::Data::new(db_manager);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(db_manager.clone())
            .service(open)
            .service(close)
            .service(store)
    })
    .bind("127.0.0.1:8080")?
    .shutdown_timeout(60)
    .run()
    .await
}
