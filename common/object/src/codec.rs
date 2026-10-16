//! 规范编码的基本类型（SPEC.md §2）：varint、bytes、string、id。

use crate::error::{Error, Result};
use crate::id::{ID_LEN, ObjectId};

/// varint 的最大字节数（u64）。
const MAX_VARINT_LEN: usize = 10;

/// 写入无符号 LEB128（总是最短编码）。
pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// ZigZag 编码后写入。
pub(crate) fn put_svarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, zigzag_encode(v));
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub(crate) fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

pub(crate) fn put_id(out: &mut Vec<u8>, id: &ObjectId) {
    out.extend_from_slice(&id.to_bytes());
}

pub(crate) fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub(crate) fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// 严格的字节读取器：拒绝非最短 varint、越界长度和尾随字节。
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or(Error::UnexpectedEof)?;
        self.pos += 1;
        Ok(b)
    }

    pub(crate) fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        if self.remaining() < N {
            return Err(Error::UnexpectedEof);
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&self.buf[self.pos..self.pos + N]);
        self.pos += N;
        Ok(out)
    }

    pub(crate) fn uvarint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for i in 0..MAX_VARINT_LEN {
            let b = self.u8()?;
            let low = u64::from(b & 0x7f);
            if i == MAX_VARINT_LEN - 1 && b > 0x01 {
                return Err(Error::VarintOverflow);
            }
            v |= low << (7 * i);
            if b & 0x80 == 0 {
                if i > 0 && b == 0 {
                    return Err(Error::VarintNonMinimal);
                }
                return Ok(v);
            }
        }
        Err(Error::VarintOverflow)
    }

    pub(crate) fn svarint(&mut self) -> Result<i64> {
        Ok(zigzag_decode(self.uvarint()?))
    }

    /// 读取元素个数。每个元素至少占 1 字节，所以个数不能超过剩余字节数，
    /// 这样可以在分配内存之前拒绝恶意的超大个数。
    pub(crate) fn count(&mut self) -> Result<usize> {
        let n = self.uvarint()?;
        if n > self.remaining() as u64 {
            return Err(Error::LengthOutOfRange(n));
        }
        Ok(n as usize)
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.uvarint()?;
        if n > self.remaining() as u64 {
            return Err(Error::LengthOutOfRange(n));
        }
        let start = self.pos;
        self.pos += n as usize;
        Ok(&self.buf[start..self.pos])
    }

    pub(crate) fn string(&mut self) -> Result<String> {
        let b = self.bytes()?;
        std::str::from_utf8(b)
            .map(str::to_owned)
            .map_err(|_| Error::InvalidUtf8)
    }

    pub(crate) fn id(&mut self) -> Result<ObjectId> {
        ObjectId::from_bytes(&self.fixed::<ID_LEN>()?)
    }

    /// 读取并校验对象头（SPEC.md §3）。
    pub(crate) fn header(&mut self, kind: u8) -> Result<()> {
        let found = self.u8()?;
        if found != kind {
            return Err(Error::WrongKind { expected: kind, found });
        }
        let ver = self.u8()?;
        if ver != crate::FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(ver));
        }
        Ok(())
    }

    /// 解码结束：不允许有剩余字节。
    pub(crate) fn finish(self) -> Result<()> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(Error::TrailingBytes(n)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::ObjectKind;
    use proptest::prelude::*;

    fn enc_u(v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, v);
        out
    }

    #[test]
    fn put_uvarint_known_values() {
        assert_eq!(enc_u(0), [0x00]);
        assert_eq!(enc_u(1), [0x01]);
        assert_eq!(enc_u(127), [0x7f]);
        assert_eq!(enc_u(128), [0x80, 0x01]);
        assert_eq!(enc_u(300), [0xac, 0x02]);
        assert_eq!(enc_u(u64::MAX).len(), MAX_VARINT_LEN);
        assert_eq!(*enc_u(u64::MAX).last().unwrap_or(&0), 0x01);
    }

    #[test]
    fn uvarint_max_roundtrips() {
        let b = enc_u(u64::MAX);
        let mut r = Reader::new(&b);
        assert_eq!(r.uvarint(), Ok(u64::MAX));
        assert_eq!(r.finish(), Ok(()));
    }

    #[test]
    fn uvarint_non_minimal_rejected() {
        assert_eq!(Reader::new(&[0x80, 0x00]).uvarint(), Err(Error::VarintNonMinimal));
        assert_eq!(Reader::new(&[0x81, 0x80, 0x00]).uvarint(), Err(Error::VarintNonMinimal));
    }

    #[test]
    fn uvarint_eleven_bytes_overflow() {
        let b = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
        assert_eq!(Reader::new(&b).uvarint(), Err(Error::VarintOverflow));
    }

    #[test]
    fn uvarint_tenth_byte_too_large_overflow() {
        let b = [0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        assert_eq!(Reader::new(&b).uvarint(), Err(Error::VarintOverflow));
    }

    #[test]
    fn uvarint_truncated_eof() {
        assert_eq!(Reader::new(&[0x80]).uvarint(), Err(Error::UnexpectedEof));
        assert_eq!(Reader::new(&[]).uvarint(), Err(Error::UnexpectedEof));
    }

    #[test]
    fn zigzag_known_values() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(i64::MAX), u64::MAX - 1);
        assert_eq!(zigzag_encode(i64::MIN), u64::MAX);
    }

    #[test]
    fn svarint_negative_roundtrips() {
        let mut out = Vec::new();
        put_svarint(&mut out, -480);
        assert_eq!(Reader::new(&out).svarint(), Ok(-480));
    }

    #[test]
    fn bytes_roundtrip_and_empty() {
        let mut out = Vec::new();
        put_bytes(&mut out, b"");
        put_bytes(&mut out, b"abc");
        let mut r = Reader::new(&out);
        assert_eq!(r.bytes(), Ok(&b""[..]));
        assert_eq!(r.bytes(), Ok(&b"abc"[..]));
        assert_eq!(r.finish(), Ok(()));
    }

    #[test]
    fn bytes_length_exceeds_input_rejected() {
        assert_eq!(Reader::new(&[0x05, b'a']).bytes(), Err(Error::LengthOutOfRange(5)));
    }

    #[test]
    fn count_exceeds_input_rejected() {
        assert_eq!(Reader::new(&[0x03, 0x00]).count(), Err(Error::LengthOutOfRange(3)));
        assert_eq!(Reader::new(&[0x00]).count(), Ok(0));
    }

    #[test]
    fn string_invalid_utf8_rejected() {
        assert_eq!(Reader::new(&[0x02, 0xc3, 0x28]).string(), Err(Error::InvalidUtf8));
    }

    #[test]
    fn string_put_roundtrips_unicode() {
        let mut out = Vec::new();
        put_str(&mut out, "模型");
        assert_eq!(Reader::new(&out).string().as_deref(), Ok("模型"));
    }

    #[test]
    fn fixed_short_input_eof() {
        assert_eq!(Reader::new(&[1, 2]).fixed::<3>(), Err(Error::UnexpectedEof));
    }

    #[test]
    fn id_roundtrips() {
        let id = ObjectId::hash(ObjectKind::Tree, b"x");
        let mut out = Vec::new();
        put_id(&mut out, &id);
        assert_eq!(Reader::new(&out).id(), Ok(id));
    }

    #[test]
    fn header_checks_kind_and_version() {
        assert_eq!(Reader::new(&[0x02, 0x01]).header(0x02), Ok(()));
        assert_eq!(
            Reader::new(&[0x03, 0x01]).header(0x02),
            Err(Error::WrongKind { expected: 2, found: 3 })
        );
        assert_eq!(
            Reader::new(&[0x02, 0x02]).header(0x02),
            Err(Error::UnsupportedVersion(2))
        );
        assert_eq!(Reader::new(&[0x02]).header(0x02), Err(Error::UnexpectedEof));
    }

    #[test]
    fn finish_trailing_bytes_rejected() {
        let mut r = Reader::new(&[0x00, 0x01, 0x02]);
        assert_eq!(r.u8(), Ok(0));
        assert_eq!(r.finish(), Err(Error::TrailingBytes(2)));
    }

    proptest! {
        #[test]
        fn uvarint_roundtrip_any(v: u64) {
            let b = enc_u(v);
            let mut r = Reader::new(&b);
            prop_assert_eq!(r.uvarint(), Ok(v));
            prop_assert_eq!(r.finish(), Ok(()));
        }

        #[test]
        fn zigzag_roundtrip_any(v: i64) {
            prop_assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }

        #[test]
        fn uvarint_never_panics_on_garbage(b in proptest::collection::vec(any::<u8>(), 0..16)) {
            let _ = Reader::new(&b).uvarint();
        }
    }
}
