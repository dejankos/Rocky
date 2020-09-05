
use std::time::Duration;
use std::{fs, io, thread};

use actix_web::dev::{ServiceResponse};
use actix_web::http::StatusCode;
use actix_web::{test, web, App, Error};

use crate::config::{DbConfig, RocksDbConfig};
use crate::conversion::bytes_to_str;

use super::*;

impl DbConfig {
    pub fn new_with_defaults() -> Self {
        DbConfig(RocksDbConfig::default())
    }
}

#[actix_rt::test]
async fn should_open_and_close_db() -> Result<(), Error> {
    std::env::set_var("RUST_BACKTRACE", "full");

    let db_manager = DbManager::new(DbConfig::new_with_defaults())?;
    let mut app = test::init_service(
        App::new()
            .app_data(web::Data::new(db_manager))
            .service(open)
            .service(close)
            .service(exists),
    )
    .await;

    let req = test::TestRequest::post().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );

    let req = test::TestRequest::delete().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );

    let req = test::TestRequest::get().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::NO_CONTENT,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );
    cleanup()?;
    Ok(())
}

#[actix_rt::test]
async fn should_add_and_delete_record() -> Result<(), Error> {
    std::env::set_var("RUST_BACKTRACE", "full");

    let db_manager = DbManager::new(DbConfig::new_with_defaults())?;
    let mut app = test::init_service(
        App::new()
            .app_data(web::Data::new(db_manager))
            .service(open)
            .service(store)
            .service(read)
            .service(remove)
            .service(close),
    )
    .await;

    let req = test::TestRequest::post().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );

    let req = test::TestRequest::post()
        .uri("/test_db/record_1")
        .set_payload("Tis but a payload")
        .to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );

    let req = test::TestRequest::get()
        .uri("/test_db/record_1")
        .to_request();
    let res = test::call_service(&mut app, req).await;
    let sc = res.status();
    let content = response_as_str(res).expect("Can't read response");
    assert_eq!(StatusCode::OK, sc, "Received payload:: {:?}", &content);
    assert_eq!(
        content, "Tis but a payload",
        "Received payload:: {:?}",
        &content
    );

    let req = test::TestRequest::delete().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );
    cleanup()?;
    Ok(())
}

#[actix_rt::test]
async fn should_expire_record() -> Result<(), Error> {
    std::env::set_var("RUST_BACKTRACE", "full");

    let db_manager = DbManager::new(DbConfig::new_with_defaults())?;
    let mut app = test::init_service(
        App::new()
            .app_data(web::Data::new(db_manager))
            .service(open)
            .service(store)
            .service(read)
            .service(remove)
            .service(close),
    )
    .await;

    let req = test::TestRequest::post().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );

    let req = test::TestRequest::post()
        .uri("/test_db/record_1")
        .set_payload("Will expire after 1 ms")
        .header("ttl", "1")
        .to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );

    thread::sleep(Duration::from_millis(5));

    let req = test::TestRequest::get()
        .uri("/test_db/record_1")
        .to_request();
    let res = test::call_service(&mut app, req).await;
    let sc = res.status();
    let content = response_as_str(res).expect("Can't read response");
    assert_eq!(
        StatusCode::NO_CONTENT,
        sc,
        "Received payload:: {:?}",
        &content
    );

    let req = test::TestRequest::delete().uri("/test_db").to_request();
    let res = test::call_service(&mut app, req).await;
    assert_eq!(
        StatusCode::OK,
        res.status(),
        "Received payload:: {:?}",
        response_as_str(res)
    );
    cleanup()?;
    Ok(())
}

fn cleanup() -> io::Result<()> {
    let config = DbConfig::new_with_defaults();
    fs::remove_dir_all(config.path())
}

fn response_as_str(res: ServiceResponse<Body>) -> Conversion<String> {
    match res.response().body().as_ref() {
        Some(Body::Bytes(bytes)) => bytes_to_str(bytes),
        _ => Ok("empty".to_string()),
    }
}
