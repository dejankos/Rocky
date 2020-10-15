use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::http::HeaderValue;

use crate::db::Data;

pub trait IntoBytes<T> {
    fn as_bytes(&self) -> bincode::Result<Vec<u8>>;
}

pub trait FromBytes<T> {
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

pub fn bytes_to_str(bytes: &[u8]) -> anyhow::Result<String> {
    Ok(String::from_utf8(bytes.to_vec())?)
}

pub fn convert(h: &HeaderValue) -> anyhow::Result<u128> {
    Ok(h.to_str()?.parse::<u128>()?)
}

pub fn current_ms() -> anyhow::Result<u128> {
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
        let res = Data::new(1, data.to_vec()).as_bytes();
        assert!(res.is_ok());
    }

    #[test]
    fn should_convert_bytes_to_struct() {
        let data = b"data";
        let res = Data::new(1, data.to_vec()).as_bytes();
        assert!(res.is_ok());

        let res = res.unwrap().as_struct();
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
