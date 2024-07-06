use std::collections::HashMap;

pub use self::date::{Date, DateDuration};
pub use self::datetime::{
    DateTime, DateTimeZoneId, DateTimeZoneIdRef, LegacyDateTime, LegacyDateTimeZoneId,
    LegacyDateTimeZoneIdRef, LocalDateTime,
};
pub use self::duration::Duration;
pub use self::node::{Node, NodeRef};
pub use self::path::{Path, PathRef, Segment};
pub use self::point::{Point2D, Point3D};
pub use self::rel::{Relationship, RelationshipRef};
pub use self::time::{LocalTime, Time};

mod date;
mod datetime;
mod de;
mod duration;
mod node;
mod path;
mod point;
mod rel;
mod time;
mod urel;

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum BoltRef<'de> {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(&'de [u8]),
    String(&'de str),
    List(Vec<BoltRef<'de>>),
    Dictionary(HashMap<&'de str, BoltRef<'de>>),
    Node(NodeRef<'de>),
    Relationship(RelationshipRef<'de>),
    Path(PathRef<'de>),
    Date(Date),
    Time(Time),
    LocalTime(LocalTime),
    DateTime(DateTime),
    DateTimeZoneId(DateTimeZoneIdRef<'de>),
    LocalDateTime(LocalDateTime),
    Duration(Duration),
    Point2D(Point2D),
    Point3D(Point3D),
    LegacyDateTime(LegacyDateTime),
    LegacyDateTimeZoneId(LegacyDateTimeZoneIdRef<'de>),
}

impl<'de> From<()> for BoltRef<'de> {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

macro_rules! impl_from_ref {
    ($case:ident($target:ty)) => {
        impl_from_ref!($case($target): $target);
    };
    ($case:ident($target:ty): $($t:ty),+ $(,)?) => {
        $(
            impl<'de> From<$t> for BoltRef<'de> {
                fn from(value: $t) -> Self {
                    Self::$case(<$target>::from(value))
                }
            }
        )*
    };
}

impl_from_ref!(Boolean(bool));
impl_from_ref!(Integer(i64): u8, u16, u32, i8, i16, i32, i64);
impl_from_ref!(Float(f64): f32, f64);
impl_from_ref!(Bytes(&'de [u8]));
impl_from_ref!(String(&'de str));
impl_from_ref!(List(Vec<BoltRef<'de>>): Vec<BoltRef<'de>>, &'de [BoltRef<'de>]);
impl_from_ref!(Dictionary(HashMap<&'de str, BoltRef<'de>>));
impl_from_ref!(Node(NodeRef<'de>));
impl_from_ref!(Relationship(RelationshipRef<'de>));
impl_from_ref!(Path(PathRef<'de>));
impl_from_ref!(Date(Date));
impl_from_ref!(Time(Time));
impl_from_ref!(LocalTime(LocalTime));
impl_from_ref!(DateTime(DateTime));
impl_from_ref!(DateTimeZoneId(DateTimeZoneIdRef<'de>));
impl_from_ref!(LocalDateTime(LocalDateTime));
impl_from_ref!(LegacyDateTime(LegacyDateTime));
impl_from_ref!(LegacyDateTimeZoneId(LegacyDateTimeZoneIdRef<'de>));
impl_from_ref!(Duration(Duration));
impl_from_ref!(Point2D(Point2D));
impl_from_ref!(Point3D(Point3D));

macro_rules! impl_try_from_int_ref {
    ($($t:ty),*) => {
        $(
            impl<'de> TryFrom<$t> for BoltRef<'de> {
                type Error = ::std::num::TryFromIntError;

                fn try_from(value: $t) -> Result<Self, Self::Error> {
                    match i64::try_from(value) {
                        Ok(value) => Ok(Self::Integer(value)),
                        Err(e) => Err(e),
                    }
                }
            }
        )*
    };
}

impl_try_from_int_ref!(u64, isize, usize, u128, i128);

impl<'de, T: Into<BoltRef<'de>> + 'de> FromIterator<T> for BoltRef<'de> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self::List(iter.into_iter().map(Into::into).collect())
    }
}

impl<'de, T: Into<BoltRef<'de>> + 'de> FromIterator<(&'de str, T)> for BoltRef<'de> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (&'de str, T)>,
    {
        Self::Dictionary(iter.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Bolt {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(bytes::Bytes),
    String(String),
    List(Vec<Bolt>),
    Dictionary(HashMap<String, Bolt>),
    Node(Node),
    Relationship(Relationship),
    Path(Path),
    Date(Date),
    Time(Time),
    LocalTime(LocalTime),
    DateTime(DateTime),
    DateTimeZoneId(DateTimeZoneId),
    LocalDateTime(LocalDateTime),
    Duration(Duration),
    Point2D(Point2D),
    Point3D(Point3D),
    LegacyDateTime(LegacyDateTime),
    LegacyDateTimeZoneId(LegacyDateTimeZoneId),
}

impl From<()> for Bolt {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

macro_rules! impl_from {
    ($case:ident($target:ty)) => {
        impl_from!($case($target): $target);
    };
    ($case:ident($target:ty): $($t:ty),+ $(,)?) => {
        $(
            impl From<$t> for Bolt {
                fn from(value: $t) -> Self {
                    Self::$case(<$target>::from(value))
                }
            }
        )*
    };
}

impl_from!(Boolean(bool));
impl_from!(Integer(i64): u8, u16, u32, i8, i16, i32, i64);
impl_from!(Float(f64): f32, f64);
impl_from!(Bytes(bytes::Bytes): bytes::Bytes, Vec<u8>);
impl_from!(String(String): String, &str);
impl_from!(List(Vec<Bolt>): Vec<Bolt>, &[Bolt]);
impl_from!(Dictionary(HashMap<String, Bolt>));
impl_from!(Node(Node));
impl_from!(Relationship(Relationship));
impl_from!(Path(Path));
impl_from!(Date(Date));
impl_from!(Time(Time));
impl_from!(LocalTime(LocalTime));
impl_from!(DateTime(DateTime));
impl_from!(DateTimeZoneId(DateTimeZoneId));
impl_from!(LocalDateTime(LocalDateTime));
impl_from!(LegacyDateTime(LegacyDateTime));
impl_from!(LegacyDateTimeZoneId(LegacyDateTimeZoneId));
impl_from!(Duration(Duration));
impl_from!(Point2D(Point2D));
impl_from!(Point3D(Point3D));

macro_rules! impl_try_from_int {
    ($($t:ty),*) => {
        $(
            impl<'de> TryFrom<$t> for Bolt {
                type Error = ::std::num::TryFromIntError;

                fn try_from(value: $t) -> Result<Self, Self::Error> {
                    match i64::try_from(value) {
                        Ok(value) => Ok(Self::Integer(value)),
                        Err(e) => Err(e),
                    }
                }
            }
        )*
    };
}

impl_try_from_int!(u64, isize, usize, u128, i128);

impl<T: Into<Bolt>> FromIterator<T> for Bolt {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self::List(iter.into_iter().map(Into::into).collect())
    }
}

impl FromIterator<(String, Bolt)> for Bolt {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (String, Bolt)>,
    {
        Self::Dictionary(iter.into_iter().collect())
    }
}

impl<'a, T: Into<Bolt>> FromIterator<(&'a str, T)> for Bolt {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, T)>,
    {
        Self::Dictionary(
            iter.into_iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect(),
        )
    }
}

impl From<BoltRef<'_>> for Bolt {
    fn from(value: BoltRef<'_>) -> Self {
        match value {
            BoltRef::Null => Self::Null,
            BoltRef::Boolean(v) => Self::Boolean(v),
            BoltRef::Integer(v) => Self::Integer(v),
            BoltRef::Float(v) => Self::Float(v),
            BoltRef::Bytes(v) => Self::Bytes(bytes::Bytes::copy_from_slice(v)),
            BoltRef::String(v) => Self::String(v.to_owned()),
            BoltRef::List(v) => Self::List(v.into_iter().map(Into::into).collect()),
            BoltRef::Dictionary(v) => Self::Dictionary(
                v.into_iter()
                    .map(|(k, v)| (k.to_owned(), v.into()))
                    .collect(),
            ),
            BoltRef::Node(v) => Self::Node(v.into_owned()),
            BoltRef::Relationship(v) => Self::Relationship(v.into_owned()),
            BoltRef::Path(v) => Self::Path(v.into_owned()),
            BoltRef::Date(v) => Self::Date(v),
            BoltRef::Time(v) => Self::Time(v),
            BoltRef::LocalTime(v) => Self::LocalTime(v),
            BoltRef::DateTime(v) => Self::DateTime(v),
            BoltRef::DateTimeZoneId(v) => Self::DateTimeZoneId(v.to_owned()),
            BoltRef::LocalDateTime(v) => Self::LocalDateTime(v),
            BoltRef::Duration(v) => Self::Duration(v),
            BoltRef::Point2D(v) => Self::Point2D(v),
            BoltRef::Point3D(v) => Self::Point3D(v),
            BoltRef::LegacyDateTime(v) => Self::LegacyDateTime(v),
            BoltRef::LegacyDateTimeZoneId(v) => Self::LegacyDateTimeZoneId(v.to_owned()),
        }
    }
}
