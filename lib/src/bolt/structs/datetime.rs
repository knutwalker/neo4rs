use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer};

use crate::bolt::structs::de::impl_visitor;

/// An instant capturing the date, the time, and the time zone.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DateTime<'de> {
    seconds: i64,
    nanoseconds: u32,
    tz_offset_seconds: i32,
    _de: PhantomData<&'de ()>,
}

impl<'de> DateTime<'de> {
    /// Seconds since Unix epoch in UTC, e.g. 0 represents 1970-01-01T00:00:01 and 1 represents 1970-01-01T00:00:02.
    pub fn seconds_since_epoch(self) -> i64 {
        self.seconds
    }

    /// Nanoseconds since midnight in the timezone of this time, not in UTC.
    pub fn nanoseconds_since_midnight(self) -> u32 {
        self.nanoseconds
    }

    /// The timezone offset in seconds from UTC.
    pub fn timezone_offset_seconds(self) -> i32 {
        self.tz_offset_seconds
    }

    // #[cfg(feature = "time_v1")]
    pub fn as_time_datetime(self) -> Option<time::OffsetDateTime> {
        let (dt, tz) =
            convert_to_time_datetime(self.seconds, self.nanoseconds, self.tz_offset_seconds)?;
        dt.checked_to_offset(tz)
    }

    pub fn as_chrono_datetime(self) -> Option<chrono::DateTime<chrono::FixedOffset>> {
        let (dt, tz) =
            convert_to_chrono_datetime(self.seconds, self.nanoseconds, self.tz_offset_seconds)?;
        Some(dt.with_timezone(&tz))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LocalDateTime<'de> {
    seconds: i64,
    nanoseconds: u32,
    _de: PhantomData<&'de ()>,
}

impl<'de> LocalDateTime<'de> {
    /// Seconds since Unix epoch, e.g. 0 represents 1970-01-01T00:00:01 and 1 represents 1970-01-01T00:00:02.
    pub fn seconds_since_epoch(self) -> i64 {
        self.seconds
    }

    /// Nanoseconds since midnight in the timezone of this time, not in UTC.
    pub fn nanoseconds_since_midnight(self) -> u32 {
        self.nanoseconds
    }

    // #[cfg(feature = "time_v1")]
    pub fn as_time_datetime(self) -> Option<time::PrimitiveDateTime> {
        let (dt, _tz) = convert_to_time_datetime(self.seconds, self.nanoseconds, 0)?;
        Some(time::PrimitiveDateTime::new(dt.date(), dt.time()))
    }

    pub fn as_chrono_datetime(self) -> Option<chrono::NaiveDateTime> {
        let (dt, _tz) = convert_to_chrono_datetime(self.seconds, self.nanoseconds, 0)?;
        Some(dt.naive_utc())
    }
}

// #[cfg(feature = "time_v1")]
fn convert_to_time_datetime(
    seconds: i64,
    nanoseconds: u32,
    tz_offset_seconds: i32,
) -> Option<(time::OffsetDateTime, time::UtcOffset)> {
    let nanos_since_epoch = i128::from(seconds).checked_mul(1_000_000_000)?;
    let nanos_since_epoch = nanos_since_epoch.checked_add(i128::from(nanoseconds))?;
    let datetime = time::OffsetDateTime::from_unix_timestamp_nanos(nanos_since_epoch).ok()?;
    let timezone = time::UtcOffset::from_whole_seconds(tz_offset_seconds).ok()?;
    Some((datetime, timezone))
}

fn convert_to_chrono_datetime(
    seconds: i64,
    nanoseconds: u32,
    tz_offset_seconds: i32,
) -> Option<(chrono::DateTime<chrono::Utc>, chrono::FixedOffset)> {
    let datetime = chrono::DateTime::from_timestamp(seconds, nanoseconds)?;
    let timezone = chrono::FixedOffset::east_opt(tz_offset_seconds)?;
    Some((datetime, timezone))
}

impl_visitor!(DateTime<'de>(seconds, nanoseconds, tz_offset_seconds { _de }) == 0x49);
impl_visitor!(LocalDateTime<'de>(seconds, nanoseconds { _de }) == 0x64);

impl<'de> Deserialize<'de> for DateTime<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("DateTime", &[], Self::visitor())
    }
}

impl<'de> Deserialize<'de> for LocalDateTime<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("LocalDateTime", &[], LocalDateTime::visitor())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, FixedOffset, Timelike};

    use crate::bolt::{
        bolt,
        packstream::{from_bytes_ref, Data},
    };

    use super::*;

    #[test]
    fn deserialize_datetime() {
        let data = bolt()
            .structure(3, 0x49)
            .int16(4500)
            .tiny_int(42)
            .int16(3600)
            .build();
        let mut data = Data::new(data);
        let date: DateTime = from_bytes_ref(&mut data).unwrap();

        let ch: chrono::DateTime<FixedOffset> = date.as_chrono_datetime().unwrap();
        assert_eq!(ch.year(), 1970);
        assert_eq!(ch.month0(), 0);
        assert_eq!(ch.day0(), 0);
        assert_eq!(ch.hour(), 2);
        assert_eq!(ch.minute(), 15);
        assert_eq!(ch.second(), 0);
        assert_eq!(ch.nanosecond(), 42);
        assert_eq!(ch.timezone().local_minus_utc(), 3600);

        let tm: time::OffsetDateTime = date.as_time_datetime().unwrap();
        assert_eq!(tm.year(), 1970);
        assert_eq!(tm.month(), time::Month::January);
        assert_eq!(tm.day(), 1);
        assert_eq!(tm.hour(), 2);
        assert_eq!(tm.minute(), 15);
        assert_eq!(tm.second(), 0);
        assert_eq!(tm.nanosecond(), 42);
        assert_eq!(tm.offset().as_hms(), (1, 0, 0));
    }

    #[test]
    fn deserialize_local_datetime() {
        let data = bolt().structure(2, 0x64).int16(4500).tiny_int(42).build();
        let mut data = Data::new(data);
        let date: LocalDateTime = from_bytes_ref(&mut data).unwrap();

        let ch: chrono::NaiveDateTime = date.as_chrono_datetime().unwrap();
        assert_eq!(ch.year(), 1970);
        assert_eq!(ch.month0(), 0);
        assert_eq!(ch.day0(), 0);
        assert_eq!(ch.hour(), 1);
        assert_eq!(ch.minute(), 15);
        assert_eq!(ch.second(), 0);
        assert_eq!(ch.nanosecond(), 42);

        let tm: time::PrimitiveDateTime = date.as_time_datetime().unwrap();
        assert_eq!(tm.year(), 1970);
        assert_eq!(tm.month(), time::Month::January);
        assert_eq!(tm.day(), 1);
        assert_eq!(tm.hour(), 1);
        assert_eq!(tm.minute(), 15);
        assert_eq!(tm.second(), 0);
        assert_eq!(tm.nanosecond(), 42);
    }

    #[test]
    fn deserialize_positive_datetime() {
        let data = bolt()
            .structure(3, 0x49)
            .int32(946_695_599)
            .int32(420_000)
            .int16(-10800)
            .build();
        let mut data = Data::new(data);
        let date: DateTime = from_bytes_ref(&mut data).unwrap();

        let ch: chrono::DateTime<FixedOffset> = date.as_chrono_datetime().unwrap();
        assert_eq!(ch.year(), 1999);
        assert_eq!(ch.month0(), 11);
        assert_eq!(ch.day0(), 30);
        assert_eq!(ch.hour(), 23);
        assert_eq!(ch.minute(), 59);
        assert_eq!(ch.second(), 59);
        assert_eq!(ch.nanosecond(), 420_000);
        assert_eq!(ch.timezone().local_minus_utc(), -10800);

        let tm: time::OffsetDateTime = date.as_time_datetime().unwrap();
        assert_eq!(tm.year(), 1999);
        assert_eq!(tm.month(), time::Month::December);
        assert_eq!(tm.day(), 31);
        assert_eq!(tm.hour(), 23);
        assert_eq!(tm.minute(), 59);
        assert_eq!(tm.second(), 59);
        assert_eq!(tm.nanosecond(), 420_000);
        assert_eq!(tm.offset().as_hms(), (-3, 0, 0));
    }

    #[test]
    fn deserialize_negative_datetime() {
        let data = bolt()
            .structure(3, 0x49)
            .int64(-16_302_076_758)
            .int32(420_000)
            .int16(10800)
            .build();
        let mut data = Data::new(data);

        let date: DateTime = from_bytes_ref(&mut data).unwrap();

        let ch: chrono::DateTime<FixedOffset> = date.as_chrono_datetime().unwrap();
        assert_eq!(ch.year(), 1453);
        assert_eq!(ch.month0(), 4);
        assert_eq!(ch.day0(), 28);
        assert_eq!(ch.hour(), 16);
        assert_eq!(ch.minute(), 20);
        assert_eq!(ch.second(), 42);
        assert_eq!(ch.nanosecond(), 420_000);
        assert_eq!(ch.timezone().local_minus_utc(), 10800);

        let tm: time::OffsetDateTime = date.as_time_datetime().unwrap();
        assert_eq!(tm.year(), 1453);
        assert_eq!(tm.month(), time::Month::May);
        assert_eq!(tm.day(), 29);
        assert_eq!(tm.hour(), 16);
        assert_eq!(tm.minute(), 20);
        assert_eq!(tm.second(), 42);
        assert_eq!(tm.nanosecond(), 420_000);
        assert_eq!(tm.offset().as_hms(), (3, 0, 0));
    }
}
