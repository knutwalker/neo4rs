use crate::{
    bolt::{ExpectedResponse, Streaming, Summary},
    errors::{Error, Result},
};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discard {
    extra: Extra,
}

impl Discard {
    pub fn all() -> Self {
        Self::new(None, None)
    }

    pub fn some(n: u32) -> Self {
        Self::new(Some(i64::from(n)), None)
    }

    pub fn many(n: u64) -> Result<Self> {
        let n = i64::try_from(n).map_err(|e| Error::IntegerOverflow("n", e))?;
        Ok(Self::new(Some(n), None))
    }

    pub fn for_query(mut self, query_id: i64) -> Self {
        self.extra.qid = Some(query_id);
        self
    }

    pub fn for_last_query(self) -> Self {
        self.for_query(-1)
    }

    fn new(how_many: Option<i64>, qid: Option<i64>) -> Self {
        let n = how_many.filter(|i| *i >= 0).unwrap_or(-1);
        let extra = Extra { n, qid };
        Discard { extra }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Extra {
    n: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    qid: Option<i64>,
}

impl Serialize for Discard {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_newtype_variant("Request", 0x2F, "DISCARD", &self.extra)
    }
}

impl ExpectedResponse for Discard {
    type Response = Summary<Streaming>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bolt::{packstream::value::bolt, Message as _, MessageResponse as _};

    #[test]
    fn serialize() {
        let hello = Discard::some(42).for_query(1);
        let bytes = hello.to_bytes().unwrap();

        let expected = bolt()
            .structure(1, 0x2F)
            .tiny_map(2)
            .tiny_string("n")
            .tiny_int(42)
            .tiny_string("qid")
            .tiny_int(1)
            .build();

        assert_eq!(bytes, expected);
    }

    #[test]
    fn serialize_default_values() {
        let hello = Discard::all();
        let bytes = hello.to_bytes().unwrap();

        let expected = bolt()
            .structure(1, 0x2F)
            .tiny_map(1)
            .tiny_string("n")
            .tiny_int(-1)
            .build();

        assert_eq!(bytes, expected);
    }

    #[test]
    fn parse() {
        let data = bolt()
            .tiny_map(1)
            .tiny_string("has_more")
            .bool(true)
            .build();

        let response = Streaming::parse(data).unwrap();

        assert_eq!(response, Streaming::HasMore);
    }
}
