use super::graph::GraphInnerMem;
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

pub struct AggregateSignal {
    pub(crate) graph: Rc<GraphInnerMem>,
    pub(crate) offsets: std::ops::Range<u16>,
}

pub struct AggregateUpdate<'a> {
    pub(crate) updated_mask: u32,
    pub(crate) signal: &'a AggregateSignal,
}

pub struct AggregateUpdateIter<'a> {
    updated_mask: u32,
    output_values: &'a [Cell<f64>],
    index_mapping: &'a [u16],
}

pub struct ModelOutput {
    fair: f64,
    bid_width: f64,
    ask_width: f64,
}

pub(crate) const MAX_SIGNALS_PER_AGGREGATE: usize = 32;

impl ConsumerSignal {
    #[inline]
    pub fn get(&self) -> f64 {
        let offset = self.which as usize;
        debug_assert!(offset < self.graph.output_values.len());
        unsafe { self.graph.output_values.get_unchecked(offset).get() }
    }
}

impl<'a> AggregateUpdate<'a> {
    #[inline]
    pub fn new(updated_mask: u32, signal: &'a AggregateSignal) -> AggregateUpdate {
        AggregateUpdate {
            updated_mask,
            signal,
        }
    }

    #[inline]
    pub fn iter_changed(&self) -> AggregateUpdateIter<'a> {
        let usize_range = (self.signal.offsets.start as usize)..(self.signal.offsets.end as usize);
        AggregateUpdateIter {
            updated_mask: self.updated_mask,
            output_values: &self.signal.graph.output_values,
            index_mapping: &self.signal.graph.aggregate_mapping_array[usize_range],
        }
    }

    #[inline]
    pub fn iter_all(&self) -> AggregateUpdateIter<'a> {
        let usize_range = (self.signal.offsets.start as usize)..(self.signal.offsets.end as usize);
        let len = self.signal.offsets.end - self.signal.offsets.start;
        debug_assert!(len <= 32);
        debug_assert!(len > 0);
        // initially set to all ones, and then mask off bits later than len
        let mask = std::u32::MAX;
        let mask_mask = ((1u64 << len) - 1) as u32;
        AggregateUpdateIter {
            updated_mask: mask & mask_mask,
            output_values: &self.signal.graph.output_values,
            index_mapping: &self.signal.graph.aggregate_mapping_array[usize_range],
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
