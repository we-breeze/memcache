use bytes::Bytes;

use crate::error::{Error, Result};

/// Flag bits used to tag stored values.
///
/// These match the constants used by the Java clients (`cn.vika.memcached`
/// `NativeHandler` and `com.schooner.MemCached`), so values written by this
/// crate can be read back by those clients and vice versa.
pub mod flags {
    /// Value body is QuickLZ-compressed (this crate cannot decode it).
    pub const F_COMPRESSED: u32 = 2;
    /// Value body is a Java-serialized object (this crate cannot decode it).
    pub const F_SERIALIZED: u32 = 8;

    /// A single byte.
    pub const MARKER_BYTE: u32 = 1;
    /// A 32-bit integer, big-endian.
    pub const MARKER_INTEGER: u32 = 4;
    /// A character, stored as a 32-bit integer.
    pub const MARKER_CHARACTER: u32 = 16;
    /// A `String`, stored as UTF-8.
    pub const MARKER_STRING: u32 = 32;
    /// A `StringBuffer`, stored as UTF-8.
    pub const MARKER_STRINGBUFFER: u32 = 64;
    /// A 16-bit short, big-endian.
    pub const MARKER_SHORT: u32 = 256;
    /// A 64-bit double, big-endian IEEE-754 bits.
    pub const MARKER_DOUBLE: u32 = 512;
    /// A date, stored as an 8-byte big-endian millisecond timestamp.
    pub const MARKER_DATE: u32 = 1024;
    /// A `StringBuilder`, stored as UTF-8.
    pub const MARKER_STRINGBUILDER: u32 = 2048;
    /// A raw byte array.
    pub const MARKER_BYTEARR: u32 = 4096;
    /// A boolean, stored as a single byte (`0`/`1`).
    pub const MARKER_BOOLEAN: u32 = 8192;
    /// A 64-bit long, big-endian.
    pub const MARKER_LONG: u32 = 16384;
}

/// A value stored in / retrieved from memcached: opaque bytes plus flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    data: Bytes,
    flags: u32,
}

impl Value {
    /// Construct a value from raw bytes and flags.
    pub fn new(data: impl Into<Bytes>, flags: u32) -> Self {
        Value {
            data: data.into(),
            flags,
        }
    }

    /// The raw stored bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consume the value and return the raw bytes.
    pub fn into_bytes(self) -> Bytes {
        self.data
    }

    /// The stored flags.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    fn reject_opaque(&self) -> Result<()> {
        if self.flags & flags::F_COMPRESSED != 0 {
            return Err(Error::Unsupported("QuickLZ compressed value"));
        }
        if self.flags & flags::F_SERIALIZED != 0 {
            return Err(Error::Unsupported("Java-serialized value"));
        }
        Ok(())
    }

    /// Decode the value as a UTF-8 string.
    ///
    /// Works for values written as `String`/`StringBuffer`/`StringBuilder`
    /// (marker bits) as well as plain untagged UTF-8 blobs.
    pub fn as_string(&self) -> Result<String> {
        self.reject_opaque()?;
        std::str::from_utf8(&self.data)
            .map(str::to_owned)
            .map_err(|err| Error::Decode(format!("value is not valid UTF-8: {err}")))
    }

    /// Decode the value as a signed 64-bit integer.
    ///
    /// Accepts 8-byte (`long`/`date`), 4-byte (`int`/`char`), 2-byte (`short`)
    /// and 1-byte (`byte`/`boolean`) big-endian encodings.
    pub fn as_i64(&self) -> Result<i64> {
        self.reject_opaque()?;
        match self.data.len() {
            8 => Ok(i64::from_be_bytes(self.data[..8].try_into().unwrap())),
            4 => Ok(i32::from_be_bytes(self.data[..4].try_into().unwrap()) as i64),
            2 => Ok(i16::from_be_bytes(self.data[..2].try_into().unwrap()) as i64),
            1 => Ok(self.data[0] as i64),
            len => Err(Error::Decode(format!(
                "cannot decode {len}-byte value as integer"
            ))),
        }
    }

    /// Decode the value as an unsigned 64-bit integer (e.g. a counter).
    pub fn as_u64(&self) -> Result<u64> {
        self.reject_opaque()?;
        // Counters set via the text protocol are stored as ASCII decimal.
        if let Ok(text) = std::str::from_utf8(&self.data)
            && let Ok(parsed) = text.trim().parse::<u64>()
        {
            return Ok(parsed);
        }
        match self.data.len() {
            8 => Ok(u64::from_be_bytes(self.data[..8].try_into().unwrap())),
            4 => Ok(u32::from_be_bytes(self.data[..4].try_into().unwrap()) as u64),
            _ => Err(Error::Decode(
                "cannot decode value as unsigned integer".into(),
            )),
        }
    }

    /// Decode the value as a boolean.
    pub fn as_bool(&self) -> Result<bool> {
        self.reject_opaque()?;
        match self.data.first() {
            Some(&byte) => Ok(byte != 0),
            None => Err(Error::Decode("empty value cannot decode as bool".into())),
        }
    }
}

/// A value together with its CAS (compare-and-swap) token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasValue {
    /// The stored value.
    pub value: Value,
    /// The CAS token to be supplied to a subsequent `cas` call.
    pub cas: u64,
}

impl CasValue {
    /// Construct a CAS value pair.
    pub fn new(value: Value, cas: u64) -> Self {
        CasValue { value, cas }
    }
}

/// Types that can be stored as a memcached value.
///
/// Numeric and boolean types are tagged with the same marker flags the Java
/// clients use, so they can be read back as the corresponding Java type. Raw
/// byte containers and strings are stored untagged (flags `0`) as generic
/// blobs, which any memcached client can read.
pub trait ToMemcacheValue {
    /// Encode `self` into a [`Value`].
    fn to_memcache_value(self) -> Value;
}

impl ToMemcacheValue for Value {
    fn to_memcache_value(self) -> Value {
        self
    }
}

impl ToMemcacheValue for &[u8] {
    fn to_memcache_value(self) -> Value {
        Value::new(Bytes::copy_from_slice(self), 0)
    }
}

impl ToMemcacheValue for Vec<u8> {
    fn to_memcache_value(self) -> Value {
        Value::new(self, 0)
    }
}

impl ToMemcacheValue for Bytes {
    fn to_memcache_value(self) -> Value {
        Value::new(self, 0)
    }
}

impl ToMemcacheValue for &str {
    fn to_memcache_value(self) -> Value {
        Value::new(Bytes::copy_from_slice(self.as_bytes()), 0)
    }
}

impl ToMemcacheValue for String {
    fn to_memcache_value(self) -> Value {
        Value::new(self.into_bytes(), 0)
    }
}

impl ToMemcacheValue for i32 {
    fn to_memcache_value(self) -> Value {
        Value::new(self.to_be_bytes().to_vec(), flags::MARKER_INTEGER)
    }
}

impl ToMemcacheValue for i64 {
    fn to_memcache_value(self) -> Value {
        Value::new(self.to_be_bytes().to_vec(), flags::MARKER_LONG)
    }
}

impl ToMemcacheValue for u64 {
    fn to_memcache_value(self) -> Value {
        Value::new((self as i64).to_be_bytes().to_vec(), flags::MARKER_LONG)
    }
}

impl ToMemcacheValue for bool {
    fn to_memcache_value(self) -> Value {
        Value::new(vec![self as u8], flags::MARKER_BOOLEAN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_roundtrip() {
        let value = "hello".to_memcache_value();
        assert_eq!(value.as_string().unwrap(), "hello");
        assert_eq!(value.as_bytes(), b"hello");
    }

    #[test]
    fn integer_roundtrip_with_marker() {
        let value = 42_i64.to_memcache_value();
        assert_eq!(value.flags(), flags::MARKER_LONG);
        assert_eq!(value.as_i64().unwrap(), 42);
    }

    #[test]
    fn bool_roundtrip_with_marker() {
        let value = true.to_memcache_value();
        assert_eq!(value.flags(), flags::MARKER_BOOLEAN);
        assert!(value.as_bool().unwrap());
    }

    #[test]
    fn compressed_and_serialized_are_unsupported() {
        let compressed = Value::new(vec![0, 1, 2], flags::F_COMPRESSED);
        assert!(matches!(compressed.as_bytes().len(), 3));
        assert!(compressed.as_string().is_err());
        let serialized = Value::new(vec![0, 1, 2], flags::F_SERIALIZED);
        assert!(serialized.as_i64().is_err());
    }

    #[test]
    fn counter_decimal_decodes_as_u64() {
        let value = Value::new(b"12345".to_vec(), 0);
        assert_eq!(value.as_u64().unwrap(), 12345);
    }
}
