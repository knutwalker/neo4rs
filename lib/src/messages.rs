mod begin;
mod failure;
mod pull;
mod record;
mod rollback;
mod run;
mod success;

use crate::{
    errors::{Error, Result},
    types::{BoltMap, BoltWireFormat},
    version::Version,
};
use begin::Begin;
use bytes::Bytes;
use failure::Failure;
use pull::Pull;
use record::Record;
use rollback::Rollback;
use run::Run;
use success::Success;

#[derive(Debug, PartialEq, Clone)]
pub enum BoltResponse {
    Success(Success),
    Failure(Failure),
    Record(Record),
}

#[derive(Debug, PartialEq, Clone)]
pub enum BoltRequest {
    Run(Run),
    Pull(Pull),
    Begin(Begin),
    Rollback(Rollback),
}

impl BoltRequest {
    pub fn run(db: &str, query: &str, params: BoltMap) -> BoltRequest {
        BoltRequest::Run(Run::new(db.into(), query.into(), params))
    }

    pub fn pull(n: usize, qid: i64) -> BoltRequest {
        BoltRequest::Pull(Pull::new(n as i64, qid))
    }

    pub fn begin(db: &str) -> BoltRequest {
        let begin = Begin::new([("db".into(), db.into())].into_iter().collect());
        BoltRequest::Begin(begin)
    }

    pub fn rollback() -> BoltRequest {
        BoltRequest::Rollback(Rollback::new())
    }
}

impl BoltRequest {
    pub fn into_bytes(self, version: Version) -> Result<Bytes> {
        let bytes: Bytes = match self {
            BoltRequest::Run(run) => run.into_bytes(version)?,
            BoltRequest::Pull(pull) => pull.into_bytes(version)?,
            BoltRequest::Begin(begin) => begin.into_bytes(version)?,
            BoltRequest::Rollback(rollback) => rollback.into_bytes(version)?,
        };
        Ok(bytes)
    }
}

impl BoltResponse {
    pub fn parse(version: Version, mut response: Bytes) -> Result<BoltResponse> {
        if Success::can_parse(version, &response) {
            let success = Success::parse(version, &mut response)?;
            return Ok(BoltResponse::Success(success));
        }
        if Failure::can_parse(version, &response) {
            let failure = Failure::parse(version, &mut response)?;
            return Ok(BoltResponse::Failure(failure));
        }
        if Record::can_parse(version, &response) {
            let record = Record::parse(version, &mut response)?;
            return Ok(BoltResponse::Record(record));
        }
        Err(Error::UnknownMessage(format!(
            "unknown message {:?}",
            response
        )))
    }

    pub fn into_error(self, msg: &'static str) -> Error {
        match self {
            BoltResponse::Failure(failure) => Error::Failure {
                code: failure.code().to_string(),
                message: failure.message().to_string(),
                msg,
            },
            _ => Error::UnexpectedMessage(format!("unexpected response for {}: {:?}", msg, self)),
        }
    }
}
