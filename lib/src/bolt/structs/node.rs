use bytes::Bytes;
use serde::de::{Deserialize, DeserializeOwned, Deserializer};

use crate::bolt::{
    packstream::{self, Data},
    structs::de::impl_visitor,
};

use super::de::{impl_visitor_ref, Keys, Single};

/// A node within the graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRef<'de> {
    id: u64,
    labels: Vec<&'de str>,
    properties: Data,
    element_id: Option<&'de str>,
}

impl<'de> NodeRef<'de> {
    /// An id for this node.
    ///
    /// Ids are guaranteed to remain stable for the duration of the session
    /// they were found in, but may be re-used for other entities after that.
    /// As such, if you want a public identity to use for your entities,
    /// attaching an explicit 'id' property or similar persistent
    /// and unique identifier is a better choice.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// A unique id for this Node.
    ///
    /// It is recommended to attach an explicit 'id' property or similar
    /// persistent and unique identifier if you want a public identity
    /// to use for your entities.
    pub fn element_id(&self) -> Option<&'de str> {
        self.element_id
    }

    /// Return all labels.
    pub fn labels(&self) -> &[&'de str] {
        &self.labels
    }

    /// Get the names of the properties of this node
    pub fn keys(&mut self) -> Vec<&str> {
        self.to::<Keys>().expect("properties should be a map").0
    }

    /// Get an attribute of this node and deserialize it into custom type that implements [`serde::Deserialize`]
    pub fn get<'this, T: Deserialize<'this> + 'this>(
        &'this mut self,
        key: &str,
    ) -> Result<Option<T>, packstream::de::Error> {
        self.properties.reset();
        packstream::from_bytes_seed(&mut self.properties, Single::new(key))
    }

    /// Deserialize the node to a type that implements [`serde::Deserialize`].
    /// The target type may borrow data from the node's properties.
    pub fn to<'this, T: Deserialize<'this> + 'this>(
        &'this mut self,
    ) -> Result<T, packstream::de::Error>
    where
        'de: 'this,
    {
        self.properties.reset();
        packstream::from_bytes_ref(&mut self.properties)
    }

    /// Convert the node into a type that implements [`serde::Deserialize`].
    /// The target type must not borrow data from the node's properties.
    pub fn into<T: DeserializeOwned>(self) -> Result<T, packstream::de::Error> {
        packstream::from_bytes(self.properties.into_inner())
    }
}

impl<'de> NodeRef<'de> {
    fn new(
        id: u64,
        labels: impl IntoIterator<Item = &'de str>,
        properties: impl Into<Bytes>,
        element_id: impl Into<Option<&'de str>>,
    ) -> Self {
        let labels = labels.into_iter().collect();
        let properties = Data::new(properties.into());
        let element_id = element_id.into();
        Self {
            id,
            labels,
            properties,
            element_id,
        }
    }
}

impl_visitor_ref!(NodeRef<'de>(id, labels, properties, [element_id]) == 0x4E);

impl<'de> Deserialize<'de> for NodeRef<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("Node", &[], Self::visitor())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use serde::Deserialize;
    use serde_test::{assert_de_tokens, Token};
    use test_case::{test_case, test_matrix};

    use crate::bolt::{
        bolt,
        packstream::{from_bytes_ref, value::BoltBytesBuilder, Data},
    };

    use super::*;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Owned {
        name: String,
        age: u32,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    struct Borrowed<'a> {
        name: Cow<'a, str>,
        age: u32,
    }

    #[test_case(tokens_v4())]
    #[test_case(tagged_tokens_v4())]
    #[test_case(tokens_v5())]
    #[test_case(tagged_tokens_v5())]
    fn tokens((tokens, element_id): (Vec<Token>, Option<&str>)) {
        let rel = NodeRef::new(42, ["Label"], properties_data(), element_id);

        assert_de_tokens(&rel, &tokens);
    }

    fn tokens_v4() -> (Vec<Token>, Option<&'static str>) {
        let properties = properties_data();
        let properties = properties.to_vec();
        let properties = Vec::leak(properties);

        (
            vec![
                Token::Seq { len: Some(3) },
                Token::U8(42),
                Token::Seq { len: Some(1) },
                Token::BorrowedStr("Label"),
                Token::SeqEnd,
                Token::BorrowedBytes(properties),
                Token::SeqEnd,
            ],
            None,
        )
    }

    fn tagged_tokens_v4() -> (Vec<Token>, Option<&'static str>) {
        let (mut tokens, element_id) = tokens_v4();
        tokens.splice(0..0, [Token::Enum { name: "Bolt" }, Token::U32(0x4E)]);
        (tokens, element_id)
    }

    fn tokens_v5() -> (Vec<Token>, Option<&'static str>) {
        let (mut tokens, _) = tokens_v4();
        tokens[0] = Token::Seq { len: Some(4) };
        let seq_end = tokens.len() - 1;
        tokens
            .splice(seq_end..seq_end, [Token::BorrowedStr("id")])
            .for_each(drop);

        (tokens, Some("id"))
    }

    fn tagged_tokens_v5() -> (Vec<Token>, Option<&'static str>) {
        let (mut tokens, element_ids) = tokens_v5();
        tokens.splice(0..0, [Token::Enum { name: "Bolt" }, Token::U32(0x4E)]);
        (tokens, element_ids)
    }

    #[test_matrix(
        [ bolt_v4(),  bolt_v5() ],
        [   owned(), borrowed() ]
    )]
    fn deserialize<T>((data, element_id): (Bytes, Option<&str>), expected: T)
    where
        T: std::fmt::Debug + PartialEq + for<'a> Deserialize<'a>,
    {
        let mut data = Data::new(data);
        let mut node: NodeRef = from_bytes_ref(&mut data).unwrap();

        assert_eq!(node.id(), 42);
        assert_eq!(node.labels(), &["Label"]);
        assert_eq!(node.element_id(), element_id);

        let properties = properties_data();
        assert_eq!(node.properties.bytes(), &properties);
        assert_eq!(node.keys(), &["age", "name"]);

        assert_eq!(node.get("age").unwrap(), Some(1337));
        assert_eq!(node.get("name").unwrap(), Some("Alice"));

        assert_eq!(node.get("missing").unwrap(), None::<String>);

        let actual: T = node.to().unwrap();
        assert_eq!(actual, expected);

        let actual: T = node.into().unwrap();
        assert_eq!(actual, expected);
    }

    fn bolt_v4() -> (Bytes, Option<&'static str>) {
        (node_test_data(3).build(), None)
    }

    fn bolt_v5() -> (Bytes, Option<&'static str>) {
        (
            node_test_data(4).tiny_string("foobar").build(),
            Some("foobar"),
        )
    }

    fn owned() -> Owned {
        Owned {
            name: "Alice".to_owned(),
            age: 1337,
        }
    }

    fn borrowed() -> Borrowed<'static> {
        Borrowed {
            name: "Alice".into(),
            age: 1337,
        }
    }

    fn node_test_data(fields: u8) -> BoltBytesBuilder {
        bolt()
            .structure(fields, 0x4E)
            .tiny_int(42)
            .tiny_list(1)
            .tiny_string("Label")
            .extend(properties_data())
    }

    fn properties_data() -> Bytes {
        bolt()
            .tiny_map(2)
            .tiny_string("age")
            .int16(1337)
            .tiny_string("name")
            .tiny_string("Alice")
            .build()
    }
}
