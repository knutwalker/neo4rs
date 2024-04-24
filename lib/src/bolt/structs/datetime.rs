use std::{marker::PhantomData, time::Duration};

use serde::de::{Deserialize, Deserializer};

use crate::bolt::structs::de::impl_visitor;

/// A date without a time-zone in the ISO-8601 calendar system.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Date<'de> {
    days: u64,
    _de: PhantomData<&'de ()>,
}

impl<'de> Date<'de> {
    /// Days since Unix epoch, e.g. 0 represents 1970-01-01 and 1 represents 1970-01-02.
    pub fn days(self) -> u64 {
        self.days
    }

    /// Returns the duration since the Unix epoch, or `None` if it overflows.
    pub fn as_duration(self) -> Option<Duration> {
        Some(Duration::from_secs(self.days.checked_mul(86400)?))
    }

    #[cfg(feature = "time_v1")]
    pub fn as_time_date(self) -> Option<time::Date> {
        time::Date::from_ordinal_date(1970, 1)
            .ok()?
            .checked_add(time::Duration::days(i64::try_from(self.days).ok()?))
    }

    pub fn as_chrono_days(self) -> chrono::Days {
        chrono::Days::new(self.days)
    }

    pub fn as_chrono_date(self) -> Option<chrono::NaiveDate> {
        chrono::NaiveDate::from_yo_opt(1970, 1)?.checked_add_days(self.as_chrono_days())
    }
}

impl From<Date<'_>> for chrono::Days {
    fn from(value: Date<'_>) -> Self {
        value.as_chrono_days()
    }
}

impl_visitor!(Date<'de>(days { _de }) == 0x44);

impl<'de> Deserialize<'de> for Date<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("Date", &[], Self::visitor())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use chrono::Datelike;

    use crate::bolt::{
        bolt,
        packstream::{from_bytes_ref, Data},
    };

    use super::*;

    #[test]
    fn deserialize() {
        let data = bolt_date();
        let mut data = Data::new(data);
        let date: Date = from_bytes_ref(&mut data).unwrap();

        assert_eq!(date.days(), 1337);
        assert_eq!(date.as_duration(), Some(Duration::from_secs(1337 * 86400)));

        let ch = date.as_chrono_date().unwrap();
        assert_eq!(ch.year(), 1973);
        assert_eq!(ch.month0(), 7);
        assert_eq!(ch.day0(), 29);
    }

    fn bolt_date() -> Bytes {
        bolt().structure(1, 0x44).int16(1337).build()
    }
}
