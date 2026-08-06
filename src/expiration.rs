use std::time::Duration;

/// Expiration policy for a stored item.
///
/// memcached interprets the on-wire expiry field as follows:
/// `0` means *never expire*; a value up to `60 * 60 * 24 * 30` (30 days) is a
/// relative number of seconds; any larger value is an absolute unix timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expiration {
    /// Never expire (wire value `0`).
    #[default]
    Never,
    /// Expire after this many seconds from now.
    Seconds(u32),
    /// Expire at this absolute unix timestamp (seconds).
    Unix(u32),
}

/// The largest value memcached still treats as a *relative* number of seconds.
const RELATIVE_MAX_SECONDS: u32 = 60 * 60 * 24 * 30;

impl Expiration {
    /// The raw value written to the protocol's expiry field.
    pub fn to_wire(self) -> u32 {
        match self {
            Expiration::Never => 0,
            Expiration::Seconds(secs) => secs,
            Expiration::Unix(ts) => ts,
        }
    }
}

impl From<u32> for Expiration {
    /// A number of seconds. `0` maps to [`Expiration::Never`]; values greater
    /// than 30 days are passed through unchanged (treated as unix timestamps by
    /// the server).
    fn from(secs: u32) -> Self {
        if secs == 0 {
            Expiration::Never
        } else if secs <= RELATIVE_MAX_SECONDS {
            Expiration::Seconds(secs)
        } else {
            Expiration::Unix(secs)
        }
    }
}

impl From<Duration> for Expiration {
    fn from(duration: Duration) -> Self {
        Expiration::from(duration.as_secs().min(u32::MAX as u64) as u32)
    }
}

impl From<Option<Duration>> for Expiration {
    fn from(duration: Option<Duration>) -> Self {
        duration.map_or(Expiration::Never, Expiration::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_seconds_is_never() {
        assert_eq!(Expiration::from(0u32), Expiration::Never);
        assert_eq!(Expiration::from(0u32).to_wire(), 0);
    }

    #[test]
    fn relative_and_absolute_boundary() {
        assert_eq!(Expiration::from(60u32), Expiration::Seconds(60));
        let over = RELATIVE_MAX_SECONDS + 1;
        assert_eq!(Expiration::from(over), Expiration::Unix(over));
    }

    #[test]
    fn duration_converts_to_seconds() {
        assert_eq!(
            Expiration::from(Duration::from_secs(90)),
            Expiration::Seconds(90)
        );
    }
}
