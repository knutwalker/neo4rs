use std::collections::HashMap;

pub use self::date::{Date, DateDuration};
pub use self::datetime::{
    DateTime, DateTimeZoneId, DateTimeZoneIdRef, LegacyDateTime, LegacyDateTimeZoneId,
    LegacyDateTimeZoneIdRef, LocalDateTime,
};
pub use self::duration::Duration;
pub use self::node::{Node, NodeRef};
pub use self::path::{PathRef, Segment};
pub use self::point::{Point2D, Point3D};
pub use self::rel::RelationshipRef;
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

macro_rules! impl_from {
    ($case:ident($target:ty)) => {
        impl_from!($case($target): $target);
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

impl_from!(Boolean(bool));
impl_from!(Integer(i64): u8, u16, u32, i8, i16, i32, i64);
impl_from!(Float(f64): f32, f64);
impl_from!(Bytes(&'de [u8]));
impl_from!(String(&'de str));
impl_from!(List(Vec<BoltRef<'de>>): Vec<BoltRef<'de>>, &'de [BoltRef<'de>]);
impl_from!(Dictionary(HashMap<&'de str, BoltRef<'de>>));
impl_from!(Node(NodeRef<'de>));
impl_from!(Relationship(RelationshipRef<'de>));
impl_from!(Path(PathRef<'de>));
impl_from!(Date(Date));
impl_from!(Time(Time));
impl_from!(LocalTime(LocalTime));
impl_from!(DateTime(DateTime));
impl_from!(DateTimeZoneId(DateTimeZoneIdRef<'de>));
impl_from!(LocalDateTime(LocalDateTime));
impl_from!(LegacyDateTime(LegacyDateTime));
impl_from!(LegacyDateTimeZoneId(LegacyDateTimeZoneIdRef<'de>));
impl_from!(Duration(Duration));
impl_from!(Point2D(Point2D));
impl_from!(Point3D(Point3D));

macro_rules! impl_try_from_int {
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

impl_try_from_int!(u64, isize, usize, u128, i128);

impl<'de> FromIterator<BoltRef<'de>> for BoltRef<'de> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = BoltRef<'de>>,
    {
        Self::List(iter.into_iter().collect())
    }
}

impl<'de> FromIterator<(&'de str, BoltRef<'de>)> for BoltRef<'de> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (&'de str, BoltRef<'de>)>,
    {
        Self::Dictionary(iter.into_iter().collect())
    }
}
