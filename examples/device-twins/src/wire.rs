//! What devices send over UDP, and what they get back. Big-endian, fixed size.
//!
//! report: `R` device:u64 seq:u32 sent_us:u64 temperature:f32   (25 bytes)
//! ack:    `A` device:u64 seq:u32                               (13 bytes)
//!
//! A device keeps one report in flight and sends it again until it is acked, so a report reaches its twin at
//! least once and the twin drops what it has already seen by `seq`.

pub const REPORT_LEN: usize = 25;
pub const ACK_LEN: usize = 13;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Report {
    pub device: u64,
    /// From 1, one more for each new reading.
    pub seq: u32,
    /// Wall clock at the device, for report-to-twin latency on one machine.
    pub sent_us: u64,
    pub temperature: f32,
}

impl Report {
    pub fn encode(&self) -> [u8; REPORT_LEN] {
        let mut out = [0u8; REPORT_LEN];
        out[0] = b'R';
        out[1..9].copy_from_slice(&self.device.to_be_bytes());
        out[9..13].copy_from_slice(&self.seq.to_be_bytes());
        out[13..21].copy_from_slice(&self.sent_us.to_be_bytes());
        out[21..25].copy_from_slice(&self.temperature.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; REPORT_LEN] = bytes.try_into().ok().filter(|b: &&[u8; REPORT_LEN]| b[0] == b'R')?;
        let (device, seq) = (u64::from_be_bytes(field(bytes, 1)), u32::from_be_bytes(field(bytes, 9)));
        let (sent_us, temperature) = (u64::from_be_bytes(field(bytes, 13)), f32::from_be_bytes(field(bytes, 21)));
        (seq > 0 && temperature.is_finite()).then_some(Self { device, seq, sent_us, temperature })
    }
}

fn field<const N: usize>(bytes: &[u8], at: usize) -> [u8; N] {
    std::array::from_fn(|i| bytes[at + i])
}

pub fn ack(device: u64, seq: u32) -> [u8; ACK_LEN] {
    let mut out = [0u8; ACK_LEN];
    out[0] = b'A';
    out[1..9].copy_from_slice(&device.to_be_bytes());
    out[9..13].copy_from_slice(&seq.to_be_bytes());
    out
}

pub fn decode_ack(bytes: &[u8]) -> Option<(u64, u32)> {
    let bytes: &[u8; ACK_LEN] = bytes.try_into().ok().filter(|b: &&[u8; ACK_LEN]| b[0] == b'A')?;
    Some((u64::from_be_bytes(field(bytes, 1)), u32::from_be_bytes(field(bytes, 9))))
}

pub fn now_us() -> u64 {
    axon::trace::unix_micros()
}

/// Hot (H) and cool (C) readings in a cycle. Every eighth device follows it, each from its own phase, so some are
/// in alert and some are not whenever the load stops; the rest stay cool.
const CYCLE: &[u8; 16] = b"HHCHHHHCCCCHHCCC";

/// The temperature device `device` reports as its `seq`th reading. A pure function, so a checker knows every alert.
pub fn temperature(device: u64, seq: u32) -> f32 {
    let wobble = (seq % 5) as f32 * 0.5;
    if !device.is_multiple_of(8) {
        return 20.0 + wobble;
    }
    let at = (u64::from(seq) + device / 8) % CYCLE.len() as u64;
    if CYCLE[at as usize] == b'H' { 90.0 + wobble } else { 20.0 + wobble }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datagrams_round_trip_and_junk_is_refused() {
        let report = Report { device: u64::MAX - 3, seq: 7, sent_us: 1_758_000_000_000_000, temperature: -4.5 };
        assert_eq!(Report::decode(&report.encode()), Some(report));
        assert_eq!(decode_ack(&ack(42, 9)), Some((42, 9)));
        let bytes = report.encode();
        assert_eq!(Report::decode(&bytes[..24]), None);
        assert_eq!(Report::decode(&[bytes.as_slice(), &[0]].concat()), None);
        assert_eq!(Report::decode(&ack(1, 1)), None);
        assert_eq!(Report::decode(&Report { seq: 0, ..report }.encode()), None);
        assert_eq!(Report::decode(&Report { temperature: f32::NAN, ..report }.encode()), None);
        assert_eq!(decode_ack(&bytes), None);
    }
}
