use std::{collections::HashMap, fmt, marker::PhantomData};

use serde::{
    de::{
        DeserializeSeed, EnumAccess, Error, IgnoredAny, MapAccess, SeqAccess, VariantAccess as _,
        Visitor,
    },
    Deserialize, Deserializer,
};

use super::{Bolt, Node};

impl<'de> Deserialize<'de> for Bolt<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Vis<'de>(PhantomData<&'de ()>);

        impl<'de> Visitor<'de> for Vis<'de> {
            type Value = Bolt<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A Bolt Node struct")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Bolt::Integer(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // TODO
                Ok(Bolt::Integer(v as i64))
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Bolt::String(v))
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Bolt::Bytes(v))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = seq.size_hint().map_or_else(Vec::new, Vec::with_capacity);
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Bolt::List(items))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut items = map
                    .size_hint()
                    .map_or_else(HashMap::new, HashMap::with_capacity);

                while let Some((key, value)) = map.next_entry::<&str, Bolt>()? {
                    items.insert(key, value);
                }
                Ok(Bolt::Dictionary(items))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: EnumAccess<'de>,
            {
                let (tag, data) = data.variant::<u8>()?;
                match tag {
                    0x4E => data.struct_variant(&[], Node::visitor()).map(Bolt::Node),
                    0x52 => todo!("Relationship"),
                    0x72 => todo!("UnboundRelationship"),
                    0x50 => todo!("Path"),
                    0x44 => todo!("Date"),
                    0x54 => todo!("Time"),
                    0x74 => todo!("LocalTime"),
                    0x49 => todo!("DateTime"),
                    0x69 => todo!("DateTimeZoneId"),
                    0x64 => todo!("LocalDateTime"),
                    0x45 => todo!("Duration"),
                    0x58 => todo!("Point2D"),
                    0x59 => todo!("Point3D"),
                    0x46 => todo!("Legacy DateTime"),
                    0x66 => todo!("Legacy DateTimeZoneId"),
                    _ => Err(Error::invalid_type(
                        serde::de::Unexpected::Other(&format!("struct with tag {tag:02X}")),
                        &"a valid Bolt struct",
                    )),
                }
            }
        }

        deserializer.deserialize_bytes(Vis(PhantomData))
    }
}

pub(super) struct Keys<'de>(pub(super) Vec<&'de str>);

impl<'de> Deserialize<'de> for Keys<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Vis<'de>(PhantomData<&'de ()>);

        impl<'de> Visitor<'de> for Vis<'de> {
            type Value = Keys<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A map of properties")
            }

            fn visit_map<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut keys = Vec::with_capacity(seq.size_hint().unwrap_or(1));
                while let Some(key) = seq.next_key()? {
                    keys.push(key);
                    let _ignore = seq.next_value::<IgnoredAny>()?;
                }
                Ok(Keys(keys))
            }
        }

        deserializer.deserialize_map(Vis(PhantomData))
    }
}

pub(super) struct Single<'a, T>(&'a str, PhantomData<T>);

impl<'a, T> Single<'a, T> {
    pub(super) fn new(key: &'a str) -> Self {
        Self(key, PhantomData)
    }
}

impl<'a, 'de, T: Deserialize<'de> + 'de> DeserializeSeed<'de> for Single<'a, T> {
    type Value = Option<T>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Key {
            Found,
            NotFound,
        }

        struct Filter<'a>(&'a str);

        impl<'a, 'de> Visitor<'de> for Filter<'a> {
            type Value = Key;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A string-like identifier of a property key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(if v == self.0 {
                    Key::Found
                } else {
                    Key::NotFound
                })
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_str(&v)
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_str(v)
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(if self.0.as_bytes() == v {
                    Key::Found
                } else {
                    Key::NotFound
                })
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_bytes(&v)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Key::NotFound)
            }
        }

        impl<'a, 'de> DeserializeSeed<'de> for Filter<'a> {
            type Value = Key;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_identifier(Filter(self.0))
            }
        }

        struct Vis<'a, 'de, T>(&'a str, PhantomData<&'de T>);

        impl<'a, 'de, T: Deserialize<'de> + 'de> Visitor<'de> for Vis<'a, 'de, T> {
            type Value = Option<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A map of properties")
            }

            fn visit_map<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut value = None::<T>;
                while let Some(key) = seq.next_key_seed(Filter(self.0))? {
                    if matches!(key, Key::Found) {
                        if value.is_some() {
                            return Err(A::Error::custom(format!("duplicate field `{}`", self.0)));
                        }
                        value = seq.next_value()?;
                    } else {
                        let _ignore = seq.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }

        deserializer.deserialize_map(Vis(self.0, PhantomData))
    }
}

macro_rules! impl_visitor {
    ($typ:ident $(<$($bound:tt),+>)? ($($name:ident),+ $(,)? $([$($opt_name:ident),+ $(,)?])?) == $tag:literal) => {
        impl$(<$($bound),+>)? $typ$(<$($bound),+>)? {
            pub(super) fn visitor() -> impl ::serde::de::Visitor<$($($bound),+,)? Value = Self> {
                struct Vis;

                impl$(<$($bound),+>)? ::serde::de::Visitor$(<$($bound),+>)? for Vis {
                    type Value = $typ$(<$($bound),+>)?;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                        formatter.write_str(concat!("a valid ", stringify!($typ), " struct"))
                    }

                    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
                    where
                        A: ::serde::de::EnumAccess<'de>,
                    {
                        let (tag, data) = data.variant::<u8>()?;
                        if tag != $tag {
                            return Err(serde::de::Error::invalid_type(
                                serde::de::Unexpected::Other(&format!("struct with tag {:02X}", tag)),
                                &format!(concat!("a Bolt ", stringify!($typ), " struct (tag {:02X})"), $tag).as_str(),
                            ));
                        }
                        ::serde::de::VariantAccess::struct_variant(data, &[], self)
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where
                        A: ::serde::de::SeqAccess<'de>,
                    {
                        $(
                            let $name = seq
                                .next_element()?
                                .ok_or_else(|| ::serde::de::Error::missing_field(stringify!($name)))?;
                        )+

                        $(
                            $(
                                let $opt_name = seq.next_element()?;
                            )+
                        )?

                        Ok($typ {
                            $($name,)+
                            $($($opt_name,)+)?
                        })
                    }
                }

                Vis
            }
        }
    };
}
pub(crate) use impl_visitor;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        let json = r#"{
            "name": "Alice",
            "age": 42,
            "email": "foo@bar.com"
        }"#;

        let keys = serde_json::from_str::<Keys>(json).unwrap();

        assert_eq!(keys.0, vec!["name", "age", "email"]);
    }

    #[test]
    fn single() {
        let json = r#"{
            "name": "Alice",
            "age": 42,
            "email": "foo@bar.com"
        }"#;

        let name = Single::<&str>::new("name")
            .deserialize(&mut serde_json::Deserializer::from_str(json))
            .unwrap();
        let age = Single::<u64>::new("age")
            .deserialize(&mut serde_json::Deserializer::from_str(json))
            .unwrap();
        let email = Single::<String>::new("email")
            .deserialize(&mut serde_json::Deserializer::from_str(json))
            .unwrap();
        let missing = Single::<bool>::new("missing")
            .deserialize(&mut serde_json::Deserializer::from_str(json))
            .unwrap();

        assert_eq!(name, Some("Alice"));
        assert_eq!(age, Some(42));
        assert_eq!(email, Some("foo@bar.com".to_owned()));
        assert_eq!(missing, None);
    }
}
