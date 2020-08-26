#[macro_use]
extern crate log;

use std::error;
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::body::{Body, ResponseBody};
use actix_web::http::header::ContentType;
use actix_web::http::HeaderValue;
use actix_web::middleware::errhandlers::{ErrorHandlerResponse, ErrorHandlers};
use actix_web::web::Bytes;
use actix_web::{delete, dev, get, http, post, put, HttpRequest, HttpResponse, ResponseError};
use actix_web::{web, App, HttpServer};
use log::LevelFilter;
use serde::Deserialize;
use simplelog::{ConfigBuilder, TermLogger, TerminalMode, ThreadLogMode, WriteLogger};
use structopt::StructOpt;

use crate::config::{load_db_config, load_service_config};
use crate::db::DbManager;
use crate::errors::{ApiError, DbError};

mod errors;

mod config;
mod db;

type Response<T> = Result<T, DbError>;
type Conversion<T> = Result<T, Box<dyn error::Error>>;

const NO_TTL: u128 = 0;
const TTL_HEADER: &str = "ttl";

#[derive(StructOpt, Debug)]
pub struct PathCfg {
    #[structopt(short, long, help = "Log files path", default_value = "./log")]
    log_path: String,
    #[structopt(
        short,
        long,
        help = "Service and database config path. Rocky will look for db_config.toml \
    and service_config.toml files under this path if not found will create config files \
    with defaults.",
        default_value = "./config"
    )]
    config_path: String,
}

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
            DbError::Config(e) => {
                HttpResponse::InternalServerError().json(ApiError::Msg(e.to_string()))
            }
        }
    }
}

fn not_found<B>(mut res: dev::ServiceResponse<B>) -> actix_web::Result<ErrorHandlerResponse<B>> {
    res.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );

    let not_found = ApiError::from_not_found(res.request().path());
    let r = res.map_body(|_h, _b| {
        ResponseBody::Other(Body::from(
            if let Ok(json) = serde_json::to_string(&not_found) {
                json
            } else {
                warn!("Can't generate not found msg - using default.");
                ApiError::not_found_generic().into()
            },
        ))
    });

    Ok(ErrorHandlerResponse::Response(r))
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
    http_res.set(ContentType::octet_stream());
    Ok(if let Some(bytes) = res {
        http_res.body(bytes)
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

pub fn current_time_ms() -> Conversion<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

// main thread will panic! if config can't be initialized
#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=error");
    std::env::set_var("RUST_BACKTRACE", "1");

    let path_cfg = PathCfg::from_args();
    let service_cfg =
        load_service_config(&path_cfg.config_path).expect("Can't load service config");
    init_logging(&path_cfg.log_path, service_cfg.dev_mode);
    info!("Running with path configuration = {:#?}", path_cfg);
    info!("Loaded service configuration = {:#?}", &service_cfg);

    let db_cfg = load_db_config(&path_cfg.config_path).expect("Can't load service config");
    info!("Loaded db configuration = {:#?}", &db_cfg);

    let db_manager = DbManager::new(db_cfg)?;
    db_manager.init();

    let db_manager = web::Data::new(db_manager);
    HttpServer::new(move || {
        App::new()
            .wrap(ErrorHandlers::new().handler(http::StatusCode::NOT_FOUND, not_found))
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

fn init_logging(log_path: &str, dev_mode: bool) {
    let cfg = ConfigBuilder::new()
        .set_thread_mode(ThreadLogMode::Both)
        .build();

    if dev_mode {
        TermLogger::init(LevelFilter::Info, cfg, TerminalMode::Mixed)
            .expect("Failed to init term logger");
    } else {
        let log_file =
            File::create(format!("{}/rocky.log", log_path)).expect("Can't create log file");
        WriteLogger::init(LevelFilter::Info, cfg, log_file).expect("Failed to init file logger")
    }
}
