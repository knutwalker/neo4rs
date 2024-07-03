use std::collections::HashMap;

pub use self::date::{Date, DateDuration};
pub use self::datetime::{
    DateTime, DateTimeZoneId, LegacyDateTime, LegacyDateTimeZoneId, LocalDateTime,
};
pub use self::node::Node;
pub use self::path::{Path, Segment};
pub use self::point::{Point2D, Point3D};
pub use self::rel::Relationship;
pub use self::time::{LocalTime, Time};

mod date;
mod datetime;
mod de;
mod node;
mod path;
mod point;
mod rel;
mod time;
mod urel;

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
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
    Path(Path<'de>),
    Date(Date<'de>),
    Time(Time<'de>),
    LocalTime(LocalTime<'de>),
    DateTime(DateTime<'de>),
    DateTimeZoneId(DateTimeZoneId<'de>),
    LocalDateTime(LocalDateTime<'de>),
    // Duration,
    Point2D(Point2D<'de>),
    Point3D(Point3D<'de>),
    LegacyDateTime(LegacyDateTime<'de>),
    LegacyDateTimeZoneId(LegacyDateTimeZoneId<'de>),
}

impl<'de> From<()> for Bolt<'de> {
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
            impl<'de> From<$t> for Bolt<'de> {
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
impl_from!(List(Vec<Bolt<'de>>): Vec<Bolt<'de>>, &'de [Bolt<'de>]);
impl_from!(Dictionary(HashMap<&'de str, Bolt<'de>>));
impl_from!(Node(Node<'de>));
impl_from!(Relationship(Relationship<'de>));
impl_from!(Path(Path<'de>));
impl_from!(Date(Date<'de>));
impl_from!(Time(Time<'de>));
impl_from!(LocalTime(LocalTime<'de>));
impl_from!(DateTime(DateTime<'de>));
impl_from!(DateTimeZoneId(DateTimeZoneId<'de>));
impl_from!(LocalDateTime(LocalDateTime<'de>));
impl_from!(LegacyDateTime(LegacyDateTime<'de>));
impl_from!(LegacyDateTimeZoneId(LegacyDateTimeZoneId<'de>));
impl_from!(Point2D(Point2D<'de>));
impl_from!(Point3D(Point3D<'de>));

macro_rules! impl_try_from_int {
    ($($t:ty),*) => {
        $(
            impl<'de> TryFrom<$t> for Bolt<'de> {
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

impl<'de> FromIterator<Bolt<'de>> for Bolt<'de> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Bolt<'de>>,
    {
        Self::List(iter.into_iter().collect())
    }
}

impl<'de> FromIterator<(&'de str, Bolt<'de>)> for Bolt<'de> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (&'de str, Bolt<'de>)>,
    {
        Self::Dictionary(iter.into_iter().collect())
    }
}
