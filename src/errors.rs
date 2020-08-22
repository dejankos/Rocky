use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::{error, fmt};

use confy::ConfyError;
use rocksdb::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum DbError {
    Rocks(Error),
    Validation(String),
    Serialization(String),
    Conversion(String),
    Config(ConfyError),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiError {
    Msg(String),
}

// TODO remove dyn
impl From<Box<dyn std::error::Error>> for DbError {
    fn from(boxed: Box<dyn error::Error>) -> Self {
        DbError::Conversion(boxed.to_string())
    }
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Rocks(e) => write!(f, "Db::RocksDb error: {}", e),
            DbError::Validation(s) => write!(f, "Db::Validation error: {}", s),
            DbError::Serialization(s) => write!(f, "Db::Serialization error: {}", s),
            DbError::Conversion(s) => write!(f, "Db::Conversion error: {}", s),
            DbError::Config(e) => write!(f, "Db::Configuration error: {}", e),
        }
    }
}

impl error::Error for DbError {
    fn cause(&self) -> Option<&dyn error::Error> {
        match self {
            DbError::Rocks(e) => Some(e),
            DbError::Validation(_) => Some(self),
            DbError::Serialization(_) => Some(self),
            DbError::Conversion(_) => Some(self),
            DbError::Config(e) => Some(e),
        }
    }
}

impl From<Error> for DbError {
    fn from(e: Error) -> Self {
        DbError::Rocks(e)
    }
}

impl From<bincode::Error> for DbError {
    fn from(e: bincode::Error) -> Self {
        DbError::Serialization(e.as_ref().to_string())
    }
}

impl From<ConfyError> for DbError {
    fn from(e: ConfyError) -> Self {
        DbError::Config(e)
    }
}

impl From<DbError> for std::io::Error {
    fn from(e: DbError) -> Self {
        std::io::Error::new(ErrorKind::InvalidData, e)
    }
}
