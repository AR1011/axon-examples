//! Keys to slots to shards, as Redis Cluster hashes them.

/// The Redis Cluster slots.
pub const SLOTS: u16 = 16384;
/// One keyed actor of kind `shard` each. Every node advertises it and refuses to start beside a node with another.
pub const SHARDS: u16 = 256;
pub const SLOTS_PER_SHARD: u16 = SLOTS / SHARDS;

/// CRC16-CCITT (XMODEM), the one Redis Cluster uses.
pub fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &b in bytes {
        crc ^= u16::from(b) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
        }
    }
    crc
}

/// The slot of a key: the CRC16 of its first non-empty `{tag}`, or of the whole key.
pub fn key_slot(key: &[u8]) -> u16 {
    let hashed = key
        .iter()
        .position(|&b| b == b'{')
        .and_then(|open| {
            let rest = &key[open + 1..];
            rest.iter().position(|&b| b == b'}').filter(|&close| close > 0).map(|close| &rest[..close])
        })
        .unwrap_or(key);
    crc16(hashed) % SLOTS
}

pub fn shard_of_slot(slot: u16) -> u16 {
    slot / SLOTS_PER_SHARD
}

pub fn shard_of(key: &[u8]) -> u16 {
    shard_of_slot(key_slot(key))
}

/// The keyed actor's key for a shard.
pub fn shard_key(shard: u16) -> String {
    format!("{shard:03}")
}

/// A hash tag whose slot is in `shard`: `{tag}` routes any key that holds it there. The first such of `t0`, `t1`, ..
pub fn tag_for(shard: u16) -> String {
    (0u32..)
        .map(|n| format!("t{n}"))
        .find(|tag| shard_of(tag.as_bytes()) == shard)
        .expect("every shard has a tag among the first few thousand")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_match_redis() {
        // From the Redis Cluster spec and `CLUSTER KEYSLOT` on a real server.
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"bar"), 5061);
        assert_eq!(key_slot(b"somekey"), 11058);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"user1000"));
        assert_eq!(key_slot(b"{user1000}.followers"), key_slot(b"user1000"));
        // An empty tag, or none closed, hashes the whole key.
        assert_eq!(key_slot(b"foo{}{bar}"), crc16(b"foo{}{bar}") % SLOTS);
        assert_eq!(key_slot(b"foo{{bar}}zap"), key_slot(b"{bar"));
        assert_eq!(key_slot(b"foo{bar"), crc16(b"foo{bar") % SLOTS);
        assert_eq!(key_slot(b""), 0);
    }

    #[test]
    fn every_shard_has_a_tag_and_64_slots() {
        assert_eq!(SLOTS_PER_SHARD, 64);
        for shard in 0..SHARDS {
            assert_eq!(shard_of(format!("probe:{{{}}}", tag_for(shard)).as_bytes()), shard);
        }
        assert_eq!(shard_of_slot(SLOTS - 1), SHARDS - 1);
    }
}
