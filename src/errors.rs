use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiError {
    Msg(String),
}

#[derive(Debug)]
pub struct ErrWrapper {
    pub err: anyhow::Error,
}

#[derive(Debug)]
pub enum ErrorCtx {
    Validation(String),
}

impl std::error::Error for ErrorCtx {}

impl Display for ErrWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(ctx) = self.err.downcast_ref::<ErrorCtx>() {
            write!(f, "{}", ctx)
        } else {
            write!(f, "{:?}", self.err)
        }
    }
}

impl Display for ErrorCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCtx::Validation(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl ApiError {
    pub fn from_not_found(path: &str) -> Self {
        ApiError::Msg(format!(
            "This is not the path you're looking for, path = \"{}\".",
            path
        ))
    }

    pub fn not_found_generic() -> Self {
        ApiError::Msg("Not found".into())
    }
}

impl From<ApiError> for String {
    fn from(e: ApiError) -> Self {
        match e {
            ApiError::Msg(s) => s,
        }
    }
}

impl From<anyhow::Error> for ErrWrapper {
    fn from(err: anyhow::Error) -> ErrWrapper {
        ErrWrapper { err }
    }
}

pub fn convert_err(any: anyhow::Error) -> std::io::Error {
    if let Some(err) = any.downcast::<std::io::Error>().err() {
        std::io::Error::new(ErrorKind::Other, err)
    } else {
        std::io::Error::from(ErrorKind::Other)
    }
}
