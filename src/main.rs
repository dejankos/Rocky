mod db;

use actix_web::{HttpRequest, HttpResponse, HttpServer, App, web};
use actix_web::get;
use actix_web::middleware::Logger;
use log::LevelFilter;
use simplelog::{Config, TerminalMode, TermLogger};
use crate::db::Db;
use std::borrow::BorrowMut;

#[get("/")]
async fn hello(
    req: HttpRequest,
    db:  web::Data<Db>
) -> String {
    db.put("a", "b");

    let option = db.get("a");

    if let Some(v) = option {
        String::from_utf8(v).unwrap_or("ups".to_string())
    }
    else {
        "nothing".to_string()
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    std::env::set_var("RUST_BACKTRACE", "1");
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap();

    let db = web::Data::new(Db::new());


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