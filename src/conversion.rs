use crate::db::Data;
use crate::Conversion;

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

#[cfg(test)]
mod tests {
    use crate::conversion::{bytes_to_str, deserialize, serialize};

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
}
