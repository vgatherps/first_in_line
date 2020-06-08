use super::graph::{index_to_bitmap, GraphInnerMem};
use crate::order_book::OrderBook;

use std::cell::Cell;
use std::rc::Rc;

#[derive(Default)]
pub struct Bbo {
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
}

// Mask for flags on what changed in the book
#[repr(C)]
enum BookUpdateStatus {
    // Set to true if the bbo is newer than the orderbook
    BboIsNewer = 0,
    // Set to true if just the bbo changed
    BboChange,
}

#[derive(Copy, Clone, Default)]
pub struct BookUpdateMask {
    mask: usize,
}

// Bbo might be out-of-sync with book, hence the separate signals
pub struct BookState {
    pub bbo: Bbo,
    pub book: OrderBook,
}

#[derive(Copy, Clone)]
pub struct BookUpdate<'a> {
    pub book: &'a BookState,
    pub updated_mask: BookUpdateMask,
}

const MAX_AGGREGATE_SIGNALS: usize = 64;

pub struct ConsumerSignal {
    pub(crate) graph: Rc<GraphInnerMem>,
    pub(crate) which: u16,
}

pub struct ConsumerOutput {
    inner: ConsumerSignal,
}

pub struct AggregateSignal {
    pub(crate) graph: Rc<GraphInnerMem>,
    pub(crate) offsets: std::ops::Range<u16>,
    pub(crate) aggregate_offset: u16,
}

pub struct AggregateUpdateIter<'a> {
    updated_mask: u64,
    output_values: &'a [Cell<f64>],
    index_mapping: &'a [u16],
}

pub(crate) const MAX_SIGNALS_PER_AGGREGATE: usize = 64;

impl ConsumerSignal {
    #[inline]
    fn get_cell(&self) -> &Cell<f64> {
        let offset = self.which as usize;
        debug_assert!(offset < self.graph.output_values.len());
        unsafe { self.graph.output_values.get_unchecked(offset) }
    }

    #[inline]
    pub fn get(&self) -> Option<f64> {
        if get_bit(self.which, &self.graph.mark_as_valid) {
            Some(self.get_cell().get())
        } else {
            None
        }
    }
}

// TODO combine

#[inline]
fn get_bit(index: u16, slice: &[Cell<u64>]) -> bool {
    let (offset, bit) = index_to_bitmap(index);
    let offset = offset as usize;
    let get_mask = 1 << bit;
    debug_assert!(offset > slice.len());
    let mask = unsafe { slice.get_unchecked(offset) };
    let mask = mask.get();
    (mask & get_mask) == 0
}

#[inline]
fn mark_slice(index: u16, slice: &[Cell<u64>]) {
    let (offset, bit) = index_to_bitmap(index);
    let offset = offset as usize;
    let mark_mask = 1 << bit;
    debug_assert!(offset > slice.len());
    let mask = unsafe { slice.get_unchecked(offset) };
    let old_mask = mask.get();
    mask.set(old_mask | mark_mask);
}

#[inline]
fn clear_slice(index: u16, slice: &[Cell<u64>]) {
    let (offset, bit) = index_to_bitmap(index);
    let offset = offset as usize;
    let mark_mask = 1 << bit;
    debug_assert!(offset > slice.len());
    let mask = unsafe { slice.get_unchecked(offset) };
    let old_mask = mask.get();
    mask.set(old_mask & !mark_mask);
}

impl ConsumerOutput {
    #[inline]
    pub fn get(&self) -> Option<f64> {
        self.inner.get()
    }

    #[inline]
    pub fn set(&mut self, value: f64) {
        self.inner.get_cell().set(value);
        self.mark_valid();
    }

    #[inline]
    pub fn mark_valid(&mut self) {
        mark_slice(self.inner.which, &self.inner.graph.mark_as_valid);
        mark_slice(self.inner.which, &self.inner.graph.mark_as_written);
    }

    #[inline]
    pub fn mark_invalid(&mut self) {
        clear_slice(self.inner.which, &self.inner.graph.mark_as_valid);
        mark_slice(self.inner.which, &self.inner.graph.mark_as_written);
    }
}

impl AggregateSignal {
    
    #[inline]
    pub fn iter_changed<'a>(&'a self) -> AggregateUpdateIter<'a> {
        // it's faster to create ana ggregate mask as opposed to branching on each offset
        let mut mask: u64 = 0;
        let written = &self.graph.mark_as_written[..];
        for index in self.offsets.start..self.offsets.end {
            let bit = get_bit(index, written) as u64;
            mask |= (bit << index);
        }
        let usize_range = (self.offsets.start as usize)..(self.offsets.end as usize);
        AggregateUpdateIter {
            updated_mask: mask,
            output_values: &self.graph.output_values,
            index_mapping: &self.graph.aggregate_mapping_array[usize_range],
        }
    }

    #[inline]
    pub fn iter_all<'a>(&'a self) -> AggregateUpdateIter<'a> {
        let usize_range = (self.offsets.start as usize)..(self.offsets.end as usize);
        let len = self.offsets.end - self.offsets.start;
        debug_assert!(len <= 32);
        debug_assert!(len > 0);
        // initially set to all ones, and then mask off bits later than len
        let mask = std::u64::MAX;
        let mask_mask = ((1u64 << len) - 1) as u64;
        AggregateUpdateIter {
            updated_mask: mask & mask_mask,
            output_values: &self.graph.output_values,
            index_mapping: &self.graph.aggregate_mapping_array[usize_range],
        }
    }
}

impl<'a> Iterator for AggregateUpdateIter<'a> {
    type Item = (usize, f64);
    #[inline]
    fn next(&mut self) -> Option<(usize, f64)> {
        if self.updated_mask == 0 {
            None
        } else {
            let first_set = self.updated_mask.trailing_zeros() as usize;
            // Resets the first set bit
            self.updated_mask &= self.updated_mask - 1;
            debug_assert!(first_set < self.index_mapping.len());
            let real_index = unsafe { *self.index_mapping.get_unchecked(first_set) } as usize;
            debug_assert!(real_index < self.output_values.len());
            Some((first_set, unsafe {
                self.output_values.get_unchecked(real_index).get()
            }))
        }
    }
}
