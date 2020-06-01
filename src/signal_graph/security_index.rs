use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub type SmallString = smallstr::SmallString<[u8; 16]>;

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Security {
    pub product: SmallString,
    pub exchange: SmallString,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct SecurityIndex {
    index: u16,
}

pub struct SecurityMap {
    securities: HashMap<Security, SecurityIndex>,
    iter_order_securities: Vec<(Security, SecurityIndex)>,
}

impl Security {
    pub fn new(product: &str, exchange: &str) -> Security {
        Security {
            product: SmallString::from_str(product),
            exchange: SmallString::from_str(exchange),
        }
    }
}

impl SecurityIndex {
    #[inline]
    pub fn get(&self) -> usize {
        self.index as usize
    }
}

static MADE_MAP: AtomicBool = AtomicBool::new(false);

// It is imperative that only one security map is ever built,
// since otherwise, a different security map could get initialized
// and give invalid indices. This is the reason for the unsafety of
// raw creation

impl SecurityMap {
    fn _new(securities: &[Security]) -> (SecurityMap, bool) {
        if securities.len() > std::u16::MAX as usize {
            panic!("Can't trade more than {} securities", std::u16::MAX);
        }
        let iter_order_securities: Vec<_> = securities
            .iter()
            .enumerate()
            .map(|(index, sec)| {
                (
                    sec.clone(),
                    SecurityIndex {
                        index: index as u16,
                    },
                )
            })
            .collect();
        let map = SecurityMap {
            securities: iter_order_securities.iter().cloned().collect(),
            iter_order_securities,
        };
        (map, MADE_MAP.swap(true, Ordering::SeqCst))
    }

    // Unsafe since you can create multiple maps this way.
    // In general only useful for testing
    pub unsafe fn new_unchecked(securities: &[Security]) -> SecurityMap {
        Self::_new(securities).0
    }

    pub fn create(securities: &[Security]) -> Arc<SecurityMap> {
        let (map_box, has_built) = Self::_new(securities);
        if has_built {
            panic!("A second global map construction attempt happened")
        }
        Arc::new(map_box)
    }

    pub fn to_index(&self, security: &Security) -> Option<SecurityIndex> {
        self.securities.get(security).map(|s| *s)
    }

    pub fn len(&self) -> usize {
        self.securities.len()
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a (Security, SecurityIndex)> {
        self.iter_order_securities.iter()
    }
}

#[cfg(test)]
mod tests {}
