//! rendezvous（最高随机权重）哈希：按对象 id 选择负责的缓存节点。
//!
//! 每个节点对每个 id 算一个分数，分数最高的节点负责这个 id。节点增减时只有原本属于（或将属于）
//! 那个节点的 id 改变归属，约 1/N。分数用 BLAKE3，不依赖进程或编译器版本，所有节点算出的结果一致。

use scion_object::ObjectId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ring {
    nodes: Vec<String>,
}

impl Ring {
    /// `nodes` 是全部缓存节点的地址（含本节点），顺序无关，重复的只算一个。
    pub fn new(nodes: impl IntoIterator<Item = String>) -> Self {
        let mut nodes: Vec<String> = nodes.into_iter().collect();
        nodes.sort();
        nodes.dedup();
        Self { nodes }
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    fn score(node: &str, id: &ObjectId) -> u64 {
        let mut h = blake3::Hasher::new();
        h.update(node.as_bytes());
        h.update(&[0]);
        h.update(&id.to_bytes());
        let d = h.finalize();
        let mut b = [0u8; 8];
        b.copy_from_slice(&d.as_bytes()[..8]);
        u64::from_le_bytes(b)
    }

    /// 负责 `id` 的节点；没有节点时为 `None`。
    pub fn owner(&self, id: &ObjectId) -> Option<&str> {
        self.nodes.iter().max_by_key(|n| Self::score(n, id)).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ids(n: u32) -> Vec<ObjectId> {
        (0..n).map(|i| ObjectId::of_blob(&i.to_le_bytes())).collect()
    }

    #[test]
    fn owner_is_deterministic_and_order_independent() {
        let a = Ring::new(["n1".to_string(), "n2".into(), "n3".into(), "n2".into()]);
        let b = Ring::new(["n3".to_string(), "n1".into(), "n2".into()]);
        assert_eq!(a, b);
        assert_eq!(a.nodes().len(), 3);
        for id in ids(100) {
            assert_eq!(a.owner(&id), b.owner(&id));
        }
        assert_eq!(Ring::new(Vec::<String>::new()).owner(&ids(1)[0]), None);
    }

    #[test]
    fn load_is_balanced_and_adding_a_node_moves_about_one_nth() {
        let three = Ring::new(["a".to_string(), "b".into(), "c".into()]);
        let four = Ring::new(["a".to_string(), "b".into(), "c".into(), "d".into()]);
        let all = ids(10_000);
        let mut load: HashMap<&str, usize> = HashMap::new();
        let mut moved = 0;
        for id in &all {
            let before = three.owner(id).expect("owner");
            *load.entry(before).or_default() += 1;
            let after = four.owner(id).expect("owner");
            if before != after {
                assert_eq!(after, "d", "ids only move to the new node");
                moved += 1;
            }
        }
        for (node, n) in load {
            assert!((2_800..3_900).contains(&n), "{node} owns {n}");
        }
        assert!((2_000..3_000).contains(&moved), "moved {moved} of 10000");
    }
}
