use std::marker::PhantomData;

use chrono::FixedOffset;
use serde::de::{Deserialize, Deserializer};
use time::UtcOffset;

use crate::bolt::structs::de::impl_visitor;

/// An instant capturing the date, the time, and the time zone.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DateTime<'de> {
    seconds: i64,
    nanoseconds: u32,
    tz_offset_seconds: i32,
    _de: PhantomData<&'de ()>,
}

impl<'de> DateTime<'de> {
    /// Seconds since Unix epoch, e.g. 0 represents 1970-01-01 and 1 represents 1970-01-02.
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
        let nanos_since_epoch = i128::from(self.seconds).checked_mul(1_000_000_000)?;
        let nanos_since_epoch = nanos_since_epoch.checked_add(i128::from(self.nanoseconds))?;
        let datetime = time::OffsetDateTime::from_unix_timestamp_nanos(nanos_since_epoch).ok()?;
        let timezone = UtcOffset::from_whole_seconds(self.tz_offset_seconds).ok()?;
        let datetime = datetime.checked_to_offset(timezone)?;
        Some(datetime)
    }

    pub fn as_chrono_datetime(self) -> Option<chrono::DateTime<FixedOffset>> {
        let datetime = chrono::DateTime::from_timestamp(self.seconds, self.nanoseconds)?;
        let timezone = FixedOffset::east_opt(self.tz_offset_seconds)?;
        let datetime = datetime.with_timezone(&timezone);
        Some(datetime)
    }
}

impl_visitor!(DateTime<'de>(seconds, nanoseconds, tz_offset_seconds { _de }) == 0x49);

impl<'de> Deserialize<'de> for DateTime<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("DateTime", &[], Self::visitor())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use chrono::{Datelike, FixedOffset, Timelike};

    use crate::bolt::{
        bolt,
        packstream::{from_bytes_ref, Data},
    };

    use super::*;

    #[test]
    fn deserialize_datetime() {
        let data = bolt_datetime();
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
        assert_eq!(ch.timezone().local_minus_utc(), -7200);

        let tm: time::OffsetDateTime = date.as_time_datetime().unwrap();
        assert_eq!(tm.year(), 1999);
        assert_eq!(tm.month(), time::Month::December);
        assert_eq!(tm.day(), 31);
        assert_eq!(tm.hour(), 23);
        assert_eq!(tm.minute(), 59);
        assert_eq!(tm.second(), 59);
        assert_eq!(tm.nanosecond(), 420_000);
        assert_eq!(tm.offset().as_hms(), (-2, 0, 0));
    }

    fn bolt_datetime() -> Bytes {
        bolt()
            .structure(3, 0x49)
            .int32(946_691_999)
            .int32(420_000)
            .int16(-7200)
            .build()
    }

    #[test]
    fn deserialize_negative_datetime() {
        let data = bolt_negative_datetime();
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

    fn bolt_negative_datetime() -> Bytes {
        bolt()
            .structure(3, 0x49)
            .int64(-16_302_076_758)
            .int32(420_000)
            .int16(10800)
            .build()
    }
}
