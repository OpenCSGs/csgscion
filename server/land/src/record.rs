//! 幂等表中保存的合入结果：请求指纹与结果编码。
//!
//! 结果编码（版本 1）：`0x01` · target(34) · ref 版本(u64 BE) · 条数(u32 BE) ·
//! 每条 original(34) landed(34) change_id(16)，然后同样格式的「早已合入」列表。

use scion_object::ObjectId;
use scion_store::RefState;

use crate::{LandError, LandOutcome, Landed, Result};

const VERSION: u8 = 1;
const ID_LEN: usize = 34;
const ENTRY_LEN: usize = ID_LEN * 2 + 16;

/// 请求参数的指纹：同一 request id 只能用于同样的 (ref, 提交, 是否只快进)。
pub(crate) fn fingerprint(reference: &str, tip: &ObjectId, fast_forward_only: bool) -> Vec<u8> {
    let mut out = b"land/v1\0".to_vec();
    out.extend_from_slice(reference.as_bytes());
    out.push(0);
    out.extend_from_slice(&tip.to_bytes());
    out.push(u8::from(fast_forward_only));
    out
}

pub(crate) fn encode(o: &LandOutcome) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + ID_LEN + 16 + (o.landed.len() + o.previously_landed.len()) * ENTRY_LEN);
    out.push(VERSION);
    out.extend_from_slice(&o.state.target.to_bytes());
    out.extend_from_slice(&o.state.version.to_be_bytes());
    for list in [&o.landed, &o.previously_landed] {
        out.extend_from_slice(&u32::try_from(list.len()).unwrap_or(u32::MAX).to_be_bytes());
        for l in list.iter() {
            out.extend_from_slice(&l.original.to_bytes());
            out.extend_from_slice(&l.landed.to_bytes());
            out.extend_from_slice(&l.change_id);
        }
    }
    out
}

pub(crate) fn decode(b: &[u8]) -> Result<LandOutcome> {
    let corrupt = |what: &str| LandError::Corrupt(format!("stored land result: {what}"));
    let mut r = Reader(b);
    if r.take(1).ok_or_else(|| corrupt("empty"))? != [VERSION] {
        return Err(corrupt("unknown version"));
    }
    let id = |r: &mut Reader<'_>| -> Result<ObjectId> {
        let raw = r.take(ID_LEN).ok_or_else(|| corrupt("truncated id"))?;
        ObjectId::from_bytes(raw).map_err(|e| corrupt(&e.to_string()))
    };
    let target = id(&mut r)?;
    let version = u64::from_be_bytes(
        r.take(8)
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| corrupt("truncated version"))?,
    );
    let mut lists = [Vec::new(), Vec::new()];
    for list in &mut lists {
        let n = u32::from_be_bytes(
            r.take(4)
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| corrupt("truncated count"))?,
        );
        for _ in 0..n {
            let original = id(&mut r)?;
            let landed = id(&mut r)?;
            let change_id = r
                .take(16)
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| corrupt("truncated change id"))?;
            list.push(Landed {
                original,
                landed,
                change_id,
            });
        }
    }
    if !r.0.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    let [landed, previously_landed] = lists;
    Ok(LandOutcome {
        state: RefState { target, version },
        landed,
        previously_landed,
    })
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_object::ObjectKind;

    fn id(n: u8) -> ObjectId {
        ObjectId::hash(ObjectKind::Changeset, &[n])
    }

    #[test]
    fn round_trip_and_rejects_corruption() {
        let o = LandOutcome {
            state: RefState {
                target: id(9),
                version: 42,
            },
            landed: vec![
                Landed {
                    original: id(1),
                    landed: id(2),
                    change_id: [3; 16],
                },
                Landed {
                    original: id(4),
                    landed: id(4),
                    change_id: [5; 16],
                },
            ],
            previously_landed: vec![Landed {
                original: id(6),
                landed: id(7),
                change_id: [8; 16],
            }],
        };
        let b = encode(&o);
        assert_eq!(decode(&b), Ok(o.clone()));
        let empty = LandOutcome {
            landed: vec![],
            previously_landed: vec![],
            ..o
        };
        assert_eq!(decode(&encode(&empty)), Ok(empty));
        for bad in [&b[..0], &b[..1], &b[..20], &b[..b.len() - 1]] {
            assert!(matches!(decode(bad), Err(LandError::Corrupt(_))), "{} bytes", bad.len());
        }
        let mut v2 = b.clone();
        v2[0] = 2;
        assert!(matches!(decode(&v2), Err(LandError::Corrupt(m)) if m.contains("version")));
        let mut trailing = b.clone();
        trailing.push(0);
        assert!(matches!(decode(&trailing), Err(LandError::Corrupt(m)) if m.contains("trailing")));
        let mut bad_id = b;
        bad_id[1] = 0xff;
        assert!(matches!(decode(&bad_id), Err(LandError::Corrupt(_))));
    }

    #[test]
    fn fingerprint_covers_every_parameter() {
        let base = fingerprint("main", &id(1), false);
        assert_ne!(base, fingerprint("dev", &id(1), false));
        assert_ne!(base, fingerprint("main", &id(2), false));
        assert_ne!(base, fingerprint("main", &id(1), true));
        // ref 名中的分隔符不会造成歧义
        assert_ne!(fingerprint("a", &id(1), false), fingerprint("a\0", &id(1), false));
    }
}
