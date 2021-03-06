use super::graph::GraphInnerMem;
use crate::order_book::OrderBook;

use std::cell::{Cell, Ref, RefCell};
use std::rc::Rc;

#[derive(Default)]
pub struct Bbo {
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
}

// Bbo might be out-of-sync with book, hence the separate signals
#[derive(Clone)]
pub struct BookViewer {
    pub(crate) book: Rc<RefCell<OrderBook>>,
}

impl BookViewer {
    pub fn book(&self) -> Ref<OrderBook> {
        self.book.borrow()
    }
}

const MAX_AGGREGATE_SIGNALS: usize = 64;

pub struct ConsumerInput {
    pub(crate) which: u16,
}

pub struct ConsumerOutput {
    pub(crate) inner: ConsumerInput,
}

pub struct ConsumerWatcher {
    pub(crate) inner: ConsumerInput,
    pub(crate) graph: Rc<GraphInnerMem>,
}

pub struct AggregateInput {
    pub(crate) offsets: std::ops::Range<u16>,
}

pub struct AggregateInputIter<'a> {
    updated_mask: u64,
    graph: &'a GraphInnerMem,
    index_mapping: &'a [u16],
}

pub(crate) const MAX_SIGNALS_PER_AGGREGATE: usize = 64;

impl ConsumerInput {
    #[inline]
    fn get_cell<'a>(&self, graph: &'a GraphInnerMem) -> &'a Cell<f64> {
        let offset = self.which as usize;
        debug_assert!(offset < graph.output_values.len());
        unsafe { graph.output_values.get_unchecked(offset) }
    }

    #[inline]
    pub fn get(&self, graph: &GraphInnerMem) -> Option<f64> {
        if self.is_valid(graph) {
            Some(self.get_cell(graph).get())
        } else {
            None
        }
    }

    #[inline]
    pub fn is_valid(&self, graph: &GraphInnerMem) -> bool {
        get_bit(self.which, VALID_MASK, &graph.mark_bitmask)
    }

    #[inline]
    pub fn was_written(&self, graph: &GraphInnerMem) -> bool {
        get_bit(self.which, WRITTEN_MASK, &graph.mark_bitmask)
    }

    #[inline]
    pub fn and(&self, other: &ConsumerInput, graph: &GraphInnerMem) -> AndConsumers<(f64, f64)> {
        AndConsumers {
            valid: get_raw_bit(self.which, VALID_MASK, &graph.mark_bitmask)
                & get_raw_bit(other.which, VALID_MASK, &graph.mark_bitmask),
            vals: (self.get_cell(graph).get(), other.get_cell(graph).get()),
        }
    }

    #[inline]
    pub fn and_out(
        &self,
        other: &ConsumerOutput,
        graph: &GraphInnerMem,
    ) -> AndConsumers<(f64, f64)> {
        self.and(&other.inner, graph)
    }
}

const WRITTEN_MASK: u8 = 1;
const VALID_MASK: u8 = 2;

// TODO combine

#[inline]
fn get_raw_bit(index: u16, mask: u8, slice: &[Cell<u8>]) -> u8 {
    let index = index as usize;
    debug_assert!(index < slice.len());
    let mask_cell = unsafe { slice.get_unchecked(index) };
    mask_cell.get() & mask
}

#[inline]
fn get_bit(index: u16, mask: u8, slice: &[Cell<u8>]) -> bool {
    get_raw_bit(index, mask, slice) != 0
}

#[inline]
fn mark_slice(index: u16, mask: u8, slice: &[Cell<u8>]) {
    let index = index as usize;
    debug_assert!(index < slice.len());
    let mask_cell = unsafe { slice.get_unchecked(index) };
    let old_mask = mask_cell.get();
    mask_cell.set(old_mask | mask);
}

#[inline]
fn clear_slice(index: u16, mask: u8, slice: &[Cell<u8>]) {
    let index = index as usize;
    debug_assert!(index < slice.len());
    let mask_cell = unsafe { slice.get_unchecked(index) };
    let old_mask = mask_cell.get();
    mask_cell.set(old_mask & !mask);
}

impl ConsumerOutput {
    #[inline]
    pub fn get(&self, graph: &GraphInnerMem) -> Option<f64> {
        self.inner.get(graph)
    }

    #[inline]
    pub fn is_valid(&self, graph: &GraphInnerMem) -> bool {
        self.inner.is_valid(graph)
    }

    #[inline]
    pub fn was_written(&self, graph: &GraphInnerMem) -> bool {
        self.inner.was_written(graph)
    }

    #[inline]
    pub fn set(&mut self, value: f64, graph: &GraphInnerMem) {
        self.set_from(Some(value), graph)
    }

    #[inline]
    pub fn set_from(&mut self, value: Option<f64>, graph: &GraphInnerMem) {
        if let Some(value) = value {
            self.inner.get_cell(graph).set(value);
            mark_slice(
                self.inner.which,
                VALID_MASK | WRITTEN_MASK,
                &graph.mark_bitmask,
            );
        } else {
            self.mark_invalid(graph)
        }
    }

    #[inline]
    fn mark_valid(&mut self, graph: &GraphInnerMem) {
        // This could be made more efficient (branchless) with some bit/shifting tricks,
        // but I don't think this is a super high-value call
        if !self.is_valid(graph) {
            mark_slice(
                self.inner.which,
                VALID_MASK | WRITTEN_MASK,
                &graph.mark_bitmask,
            );
        }
    }

    #[inline]
    pub fn mark_invalid(&mut self, graph: &GraphInnerMem) {
        // This could be made more efficient (branchless) with some bit/shifting tricks,
        // but I don't think this is a super high-value call
        if self.is_valid(graph) {
            clear_slice(
                self.inner.which,
                VALID_MASK | WRITTEN_MASK,
                &graph.mark_bitmask,
            );
        }
    }
}

impl ConsumerWatcher {
    #[inline]
    pub fn get(&self) -> Option<f64> {
        self.inner.get(&*self.graph)
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid(&*self.graph)
    }

    #[inline]
    pub fn was_written(&self) -> bool {
        self.inner.was_written(&*self.graph)
    }
}

pub struct AggregateInputGenerator {
    pub(crate) mapping: Vec<u16>,
    pub(crate) offsets: std::ops::Range<u16>,
}

impl AggregateInputGenerator {
    pub fn as_consumers(&self) -> Vec<ConsumerInput> {
        self.offsets
            .clone()
            .map(|o| ConsumerInput {
                which: self.mapping[o as usize],
            })
            .collect()
    }

    pub fn as_update(&self) -> AggregateInput {
        AggregateInput {
            offsets: self.offsets.clone(),
        }
    }
}

impl AggregateInput {
    #[inline]
    pub fn iter_changed<'a>(&self, graph: &'a GraphInnerMem) -> AggregateInputIter<'a> {
        // it's faster to create an aggregate mask as opposed to branching on each offset
        let mut mask: u64 = 0;
        let written = &graph.mark_bitmask[..];
        for index in self.offsets.start..self.offsets.end {
            let bit = get_bit(index, WRITTEN_MASK, written) as u64;
            mask |= (bit << index);
        }
        let usize_range = (self.offsets.start as usize)..(self.offsets.end as usize);
        AggregateInputIter {
            updated_mask: mask,
            graph,
            index_mapping: &graph.aggregate_mapping_array[usize_range],
        }
    }
}

impl<'a> Iterator for AggregateInputIter<'a> {
    type Item = (usize, Option<f64>);
    #[inline]
    fn next(&mut self) -> Option<(usize, Option<f64>)> {
        if self.updated_mask == 0 {
            None
        } else {
            let first_set = self.updated_mask.trailing_zeros() as usize;
            // Resets the first set bit
            self.updated_mask &= self.updated_mask - 1;
            debug_assert!(first_set < self.index_mapping.len());
            let real_index = unsafe { *self.index_mapping.get_unchecked(first_set) } as usize;
            debug_assert!(real_index < self.graph.output_values.len());
            let cons = ConsumerInput {
                which: real_index as u16,
            };
            Some((first_set, cons.get(self.graph)))
        }
    }
}

mod private {
    pub trait Seal {}
}

pub trait TupleNext: private::Seal {
    type Next;
    fn join_next(&self, val: f64) -> Self::Next;
}

pub struct AndConsumers<T> {
    vals: T,
    valid: u8,
}

impl<T: Copy> AndConsumers<T> {
    #[inline]
    pub fn get(&self) -> Option<T> {
        if self.valid != 0 {
            Some(self.vals)
        } else {
            None
        }
    }
}

impl<T: TupleNext + Copy> AndConsumers<T> {
    #[inline]
    pub fn and(&self, other: &ConsumerInput, graph: &GraphInnerMem) -> AndConsumers<T::Next> {
        let new_valid = get_raw_bit(other.which, VALID_MASK, &graph.mark_bitmask) & self.valid;
        let next = other.get_cell(graph).get();
        AndConsumers {
            vals: self.vals.join_next(next),
            valid: new_valid,
        }
    }

    #[inline]
    pub fn and_out(&self, other: &ConsumerOutput, graph: &GraphInnerMem) -> AndConsumers<T::Next> {
        self.and(&other.inner, graph)
    }
}

macro_rules! impl_combined_next {
    ($( $t:ty )*) => {
        impl private::Seal for ( $( $t ),* ) {}
        impl TupleNext for ( $( $t ),* ) {
            type Next = <( f64, $( $t ),* ) as tuple::OpRotateLeft>::Output;
            fn join_next(&self, val: f64) -> Self::Next {
                use tuple::OpJoin;
                self.join((val,))
            }
        }
    };
}

impl_combined_next!(f64 f64);
impl_combined_next!(f64 f64 f64);
impl_combined_next!(f64 f64 f64 f64);
impl_combined_next!(f64 f64 f64 f64 f64);
impl_combined_next!(f64 f64 f64 f64 f64 f64);
impl_combined_next!(f64 f64 f64 f64 f64 f64 f64);

pub trait InputType: private::Seal + std::any::Any {}
pub trait FloatAggregate: private::Seal {}

impl private::Seal for BookViewer {}
impl InputType for BookViewer {}

impl private::Seal for ConsumerInput {}
impl InputType for ConsumerInput {}

impl private::Seal for AggregateInputGenerator {}
impl InputType for AggregateInputGenerator {}
