#[macro_use]
extern crate log;

use std::error;

use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::http::header::ContentType;
use actix_web::http::HeaderValue;
use actix_web::middleware::Logger;
use actix_web::web::Bytes;
use actix_web::{delete, get, post, put, HttpRequest, HttpResponse, ResponseError};
use actix_web::{web, App, HttpServer};
use log::LevelFilter;
use serde::{Deserialize};
use simplelog::{Config, TermLogger, TerminalMode};

use crate::config::load_db_config;
use crate::db::DbManager;
use crate::errors::{ApiError, DbError};

mod errors;

mod config;
mod db;

type Response<T> = Result<T, DbError>;
type Conversion<T> = Result<T, Box<dyn error::Error>>;

const NO_TTL: u128 = 0;
const TTL_HEADER: & str = "ttl";

#[derive(Deserialize)]
struct PathVal {
    db_name: String,
    key: String,
}

trait Expiration {
    fn calc_expire(&self) -> Conversion<u128>;
}

impl Expiration for HttpRequest {
    fn calc_expire(&self) -> Conversion<u128> {
        self.headers()
            .get(TTL_HEADER)
            .map(|h| Ok(current_time_ms()? + convert(h)?))
            .unwrap_or(Ok(NO_TTL))
    }
}

impl ResponseError for DbError {
    fn error_response(&self) -> HttpResponse {
        match self {
            DbError::Validation(s) | DbError::Serialization(s) | DbError::Conversion(s) => {
                HttpResponse::BadRequest().json(ApiError::Msg(s.into()))
            }
            DbError::Rocks(e) => {
                HttpResponse::InternalServerError().json(ApiError::Msg(e.to_string()))
            }
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
    req: HttpRequest,
    db_man: web::Data<DbManager>,
) -> Response<HttpResponse> {
    db_man
        .store(
            p_val.db_name.as_str(),
            p_val.key.as_str(),
            body,
            req.calc_expire()?,
        )
        .await?;
    Ok(HttpResponse::Ok().finish())
}

#[get("/{db_name}/{key}")]
async fn read(p_val: web::Path<PathVal>, db_man: web::Data<DbManager>) -> Response<HttpResponse> {
    let res = db_man
        .read(p_val.db_name.as_str(), p_val.key.as_str())
        .await?;

    let mut http_res = HttpResponse::Ok();
    Ok(if let Some(bytes) = res {
        http_res.set(ContentType::octet_stream()).body(bytes)
    } else {
        http_res.finish()
    })
}

#[delete("/{db_name}/{key}")]
async fn remove(p_val: web::Path<PathVal>, db_man: web::Data<DbManager>) -> Response<HttpResponse> {
    db_man
        .remove(p_val.db_name.as_str(), p_val.key.as_str())
        .await?;
    Ok(HttpResponse::Ok().finish())
}

fn convert(h: &HeaderValue) -> Conversion<u128> {
    Ok(h.to_str()?.parse::<u128>()?)
}

fn current_time_ms() -> Conversion<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
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
            .service(read)
            .service(remove)
    })
    .bind("127.0.0.1:8080")?
    .shutdown_timeout(60)
    .run()
    .await
}
