use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::http::HeaderValue;

use crate::db::Data;
use std::error;

pub type Conversion<T> = Result<T, Box<dyn error::Error>>;

pub trait IntoBytes<T> {
    fn as_bytes(&self) -> bincode::Result<Vec<u8>>;
}

trait FromBytes<T> {
    fn as_struct(&self) -> bincode::Result<T>;
}

impl IntoBytes<Data> for Data {
    fn as_bytes(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(self)
    }
}

impl FromBytes<Data> for Vec<u8> {
    fn as_struct(&self) -> bincode::Result<Data> {
        bincode::deserialize(self)
    }
}

pub fn serialize(data: Vec<u8>, ttl: u128) -> Conversion<Vec<u8>> {
    Ok(Data::new(ttl, data).as_bytes()?)
}

pub fn deserialize(data: Vec<u8>) -> Conversion<Data> {
    Ok(data.as_struct()?)
}

pub fn bytes_to_str(bytes: &[u8]) -> Conversion<String> {
    Ok(String::from_utf8(bytes.to_vec())?)
}

pub fn convert(h: &HeaderValue) -> Conversion<u128> {
    Ok(h.to_str()?.parse::<u128>()?)
}

pub fn current_ms() -> Conversion<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_bytes_to_str() {
        let res = bytes_to_str(b"I'm a &str");
        assert!(res.is_ok());
        assert_eq!("I'm a &str", res.unwrap());
    }

    #[test]
    fn should_convert_struct_to_bytes() {
        let data = b"data";
        let res = serialize(data.to_vec(), 1);
        assert!(res.is_ok());
    }

    #[test]
    fn should_convert_bytes_to_struct() {
        let data = b"data";
        let res = serialize(data.to_vec(), 1);
        assert!(res.is_ok());

        let res = deserialize(res.unwrap());
        assert!(res.is_ok());
    }

    #[test]
    fn should_convert_header() {
        let header_val = convert(&HeaderValue::from_str("42").unwrap());
        assert_eq!(42, header_val.unwrap());

        let header_val = convert(&HeaderValue::from(42));
        assert_eq!(42, header_val.unwrap());

        let header_val = convert(&HeaderValue::from_bytes(b"42").unwrap());
        assert_eq!(42, header_val.unwrap());
    }
}
