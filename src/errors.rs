pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IOError { detail: String },
    ConnectionError,
    StringTooLong,
    MapTooBig,
    ListTooLong,
    UnexpectedMessage,
    QueryError,
    UnknownType { detail: String },
    UnknownMessage,
    ConverstionError,
    AuthenticationError { detail: String },
    InvalidMessageMarker { detail: String },
    InvalidTypeMarker { detail: String },
    DeserializationError { detail: String },
}

impl std::convert::From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IOError {
            detail: e.to_string(),
        }
    }
}

impl std::convert::From<deadpool::managed::PoolError<Error>> for Error {
    fn from(e: deadpool::managed::PoolError<Error>) -> Self {
        match e {
            deadpool::managed::PoolError::Backend(e) => e,
            _ => Error::ConnectionError,
        }
    }
}
