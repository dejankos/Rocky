#[macro_use]
extern crate log;

mod config;
mod db;

use crate::db::{Db, DbManager};
use actix_web::middleware::Logger;
use actix_web::{post, HttpResponse};
use actix_web::{web, App, HttpServer};
use log::LevelFilter;
use simplelog::{Config, TermLogger, TerminalMode};

use crate::config::load_db_config;

#[post("/{db_name}")]
async fn open(db_name: web::Path<String>, db_man: web::Data<DbManager>) -> HttpResponse {
    db_man.open(db_name.into_inner()).unwrap();
    info!("after open ");

    HttpResponse::Ok().finish()
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    std::env::set_var("RUST_BACKTRACE", "1");
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap();

    let db_cfg = load_db_config().expect("Failed to start - can't load db config");
    info!("Db config = {:?}", db_cfg);

    let db = web::Data::new(Db::new(db_cfg.path.as_str()));
    let db_manager = DbManager::new(db_cfg).expect("handle err");
    db_manager.init().expect("handle err");
    let db_manager = web::Data::new(db_manager);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(db.clone())
            .app_data(db_manager.clone())
            .service(open)
    })
    .bind("127.0.0.1:8080")?
    .shutdown_timeout(60)
    .run()
    .await
}
