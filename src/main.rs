#[macro_use]
extern crate log;

mod config;
mod db;

use crate::db::Db;
use actix_web::get;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpRequest, HttpServer};
use log::LevelFilter;
use simplelog::{Config, TermLogger, TerminalMode};

use crate::config::load_db_config;

#[get("/")]
async fn hello(_req: HttpRequest, db: web::Data<Db>) -> String {
    db.put("a", "b");

    let option = db.get("a");

    if let Some(v) = option {
        String::from_utf8(v).unwrap_or("ups".to_string())
    } else {
        "nothing".to_string()
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    std::env::set_var("RUST_BACKTRACE", "1");
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap();

    let db_cfg = load_db_config().expect("Failed to start - can't load db config");
    info!("Db config = {:?}", db_cfg);

    let db = web::Data::new(Db::new(db_cfg.path.as_str()));

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(db.clone())
            .service(hello)
    })
    .bind("127.0.0.1:8080")?
    .shutdown_timeout(10)
    .run()
    .await
}
