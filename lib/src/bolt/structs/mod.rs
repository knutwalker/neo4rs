use std::collections::HashMap;

pub use self::node::Node;
pub use self::rel::Relationship;
pub use self::urel::UnboundRelationship;

mod de;
mod node;
mod rel;
mod urel;

#[derive(Clone, Debug, PartialEq)]
pub enum Bolt<'de> {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(&'de [u8]),
    String(&'de str),
    List(Vec<Bolt<'de>>),
    Dictionary(HashMap<&'de str, Bolt<'de>>),
    Node(Node<'de>),
    Relationship(Relationship<'de>),
    UnboundRelationship(UnboundRelationship<'de>),
    Path,
    Date,
    Time,
    LocalTime,
    DateTime,
    DateTimeZoneId,
    LocalDateTime,
    Duration,
    Point2D,
    Point3D,
    LegacyDateTime,
    LegacyDateTimeZoneId,
}

impl<'de> Bolt<'de> {
    fn null() -> Self {
        Bolt::Null
    }

    fn boolean(b: bool) -> Self {
        Bolt::Boolean(b)
    }

    fn integer(i: impl Into<i64>) -> Self {
        Bolt::Integer(i.into())
    }

    fn float(f: impl Into<f64>) -> Self {
        Bolt::Float(f.into())
    }

    fn bytes(b: impl Into<&'de [u8]>) -> Self {
        Bolt::Bytes(b.into())
    }

    fn string(s: impl Into<&'de str>) -> Self {
        Bolt::String(s.into())
    }

    fn list<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Bolt<'de>>,
    {
        Bolt::List(items.into_iter().collect())
    }

    fn dict<K>(items: impl IntoIterator<Item = (K, Bolt<'de>)>) -> Self
    where
        K: Into<&'de str>,
    {
        Bolt::Dictionary(items.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }

    fn node(node: Node<'de>) -> Self {
        Self::Node(node)
    }

    fn rel(rel: Relationship<'de>) -> Self {
        Self::Relationship(rel)
    }

    fn urel(urel: UnboundRelationship<'de>) -> Self {
        Self::UnboundRelationship(urel)
    }
}
