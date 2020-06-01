use crate::signal_graph::security_index::{Security, SecurityIndex, SecurityMap};

// A specialized vector interface that holds exactly one element per
// security. Using a security index here will always be valid
pub struct SecurityVector<T> {
    elems: Vec<T>,
}

impl<T: Default> SecurityVector<T> {
    pub fn new(map: &SecurityMap) -> Self {
        Self::new_with(map, |_, _| T::default())
    }
}

impl<T: Clone> SecurityVector<T> {
    pub fn new_from(map: &SecurityMap, from: T) -> Self {
        Self::new_with(map, |_, _| from.clone())
    }
}

impl<T> SecurityVector<T> {
    pub fn new_with<F: FnMut(&Security, SecurityIndex) -> T>(map: &SecurityMap, mut f: F) -> Self {
        Self {
            elems: map.iter().map(|(sec, ind)| f(sec, *ind)).collect(),
        }
    }

    pub fn new_with_err<E, F: FnMut(&Security, SecurityIndex) -> Result<T, E>>(
        map: &SecurityMap,
        mut f: F,
    ) -> Result<Self, E> {
        let mut elems = Vec::new();
        for (sec, ind) in map.iter() {
            elems.push(f(sec, *ind)?);
        }
        Ok(Self { elems })
    }

    pub fn get(&self, index: SecurityIndex) -> &T {
        let index = index.get();
        debug_assert!(index < self.elems.len());
        unsafe { self.elems.get_unchecked(index) }
    }

    pub fn get_mut(&mut self, index: SecurityIndex) -> &mut T {
        let index = index.get();
        debug_assert!(index < self.elems.len());
        unsafe { self.elems.get_unchecked_mut(index) }
    }
}
