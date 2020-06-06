use super::interface_types::*;
use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::graph::{
    ConsumerOrAggregate, Graph, GraphAggregateData, GraphCallList, GraphInnerMem, GraphObjectStore,
};
use super::graph_error::GraphError;
use super::graph_sort::generate_calls_for;
use super::security_data::SecurityVector;
use super::security_index::{Security, SecurityMap};

pub struct GraphRegistrar {
    signal_definitions: HashMap<&'static str, SignalDefinition>,
}

#[derive(Clone)]
pub struct SignalDefinition {
    pub(crate) inputs: HashMap<&'static str, Signal>,
    pub(crate) creator: fn(
        consumers: HashMap<&'static str, ConsumerSignal>,
        aggregators: HashMap<&'static str, Vec<AggregateSignal>>,
        objects: &mut dynstack::DynStack<dyn RawObj>,
    ) -> u16,
}

#[derive(Clone)]
pub(crate) struct SignalInstantiation {
    pub(crate) definition: SignalDefinition,
    pub(crate) inputs: HashMap<String, NamedSignalType>,
}

#[derive(Clone)]
pub struct Signal {
    pub sig_type: InputSignalType,
    pub type_check: fn(&dyn Any),
}

#[derive(Clone)]
pub enum NamedSignalType {
    Book(Security),
    Inner(bool, Vec<String>),
}

#[derive(Clone)]
pub enum InputSignalType {
    Book(BookCallback),
    Inner(bool, InnerCallback),
}

// TODO should be called instantiation, name already taken
pub struct SignalCall {
    pub signal_name: String,
    pub inputs: HashMap<String, NamedSignalType>,
}

impl GraphRegistrar {
    pub fn new(
        signal_definition_list: &[(&'static str, SignalDefinition)],
    ) -> Result<Self, GraphError> {
        let mut signal_definitions = HashMap::new();
        for (name, definition) in signal_definition_list.iter().map(|(n, d)| (*n, d.clone())) {
            if signal_definitions.insert(name, definition).is_some() {
                return Err(GraphError::DuplicateSignalName(name));
            }
        }
        Ok(GraphRegistrar { signal_definitions })
    }

    pub fn generate_graph(
        &self,
        layout: &[(String, SignalCall)],
        security_map: &SecurityMap,
    ) -> Result<Graph, GraphError> {
        let mut signal_to_instance = HashMap::new();
        for (name, call) in layout {
            let call_sig: &str = &call.signal_name;
            if signal_to_instance.contains_key(name) {
                return Err(GraphError::DuplicateSignalInstance(name.clone()));
            }
            let definition = if let Some(def) = self.signal_definitions.get(call_sig) {
                def.clone()
            } else {
                return Err(GraphError::DefinitionNotFound {
                    definition: call.signal_name.clone(),
                    signal: name.clone(),
                });
            };
            signal_to_instance.insert(
                name.clone(),
                SignalInstantiation {
                    inputs: call.inputs.clone(),
                    definition,
                },
            );
        }

        let inner_mem = GraphInnerMem::new(signal_to_instance, layout, security_map)?;
        let objects = Rc::new(GraphObjectStore::new(inner_mem.clone())?);
        let requested_book_signals: HashSet<_> = layout
            .iter()
            .map(|(_, b)| b)
            .flat_map(|sc| sc.inputs.values())
            .filter_map(|nst| match nst {
                NamedSignalType::Book(sec) => Some(sec),
                _ => None,
            })
            .collect();

        for security in &requested_book_signals {
            let security = *security;
            if security_map.to_index(security).is_none() {
                return Err(GraphError::BookNotFound {
                    security: security.clone(),
                });
            }
        }

        let aggregate = GraphAggregateData::new(inner_mem.clone());

        let security_call_list = SecurityVector::new_with_err(security_map, |sec, _| {
            if requested_book_signals.contains(sec) {
                Some(generate_calls_for(
                    sec,
                    inner_mem.clone(),
                    &aggregate,
                    objects.clone(),
                ))
                .transpose()
            } else {
                Ok(None)
            }
        })?;

        Ok(Graph {
            book_updates: security_call_list,
            agg_data: aggregate,
            objects,
            mem: inner_mem,
        })
    }
}

pub trait RegisterSignal {
    // DO NOT CHANGE THIS REQUIREMENT - 'static is important here for safety
    type Child: 'static + Sized;

    fn get_signals() -> HashMap<&'static str, Signal>;
    fn create(
        consumers: HashMap<&'static str, ConsumerSignal>,
        aggregators: HashMap<&'static str, Vec<AggregateSignal>>,
    ) -> Self::Child;

    fn get_definition() -> SignalDefinition {
        SignalDefinition {
            inputs: Self::get_signals(),
            creator: Self::_real_create,
        }
    }

    fn _real_create(
        consumers: HashMap<&'static str, ConsumerSignal>,
        aggregators: HashMap<&'static str, Vec<AggregateSignal>>,
        objects: &mut dynstack::DynStack<dyn RawObj>,
    ) -> u16 {
        let val = Self::create(consumers, aggregators);
        let index = objects.len();
        dynstack::dyn_push!(objects, UnsafeCell::new(val));
        let val = &objects[index];
        assert!(index < std::u16::MAX as usize);
        index as u16
    }
}

pub trait RawObj {
    fn get_addr(&self) -> *mut u8;
    fn check_addr(&self, ptr: *mut u8) {
        assert_eq!(ptr, self.get_addr());
    }
}

impl<T: 'static + Sized> RawObj for UnsafeCell<T> {
    fn get_addr(&self) -> *mut u8 {
        self.get() as *mut u8
    }
}

pub unsafe trait _CallSignal_ {
    type Item: 'static + Any + Sized + RegisterSignal;
    const TYPE_NAME: &'static str;
    fn check_type(any: &dyn Any) {
        if !any.is::<Self::Item>() {
            panic!(
                "Wrong book call type detected: got type {:?}, expected {}",
                any.type_id(),
                Self::TYPE_NAME
            );
        }
    }

    // These allow us to tag dereferenced pointers with extremely short lifetimes
    // local to the function scope, so nobody can put the references somewhere else

    #[inline]
    fn shrink_lifetime<'a, T>(ptr: *const T, _: &'a usize) -> &'a T {
        unsafe { &*ptr }
    }

    #[inline]
    fn shrink_lifetime_mut<'a, T>(ptr: *mut T, _: &'a usize) -> &'a mut T {
        unsafe { &mut *ptr }
    }
}

#[macro_export]
macro_rules! make_consumer_callback {
    ($fn_name: ident, $consumer_type: ty) => {{
        use std::cell::Cell;
        struct InnerCallback {}
        unsafe impl _CallSignal_ for InnerCallback {
            type Item = $consumer_type;
            const TYPE_NAME: &'static str = stringify!($consumer_type);
        }
        use arby::signal_graph::graph::ConsumerOrAggregate;
        impl InnerCallback {
            fn redirect_call(
                ptr: *mut u8,
                index: ConsumerOrAggregate,
                time: u64,
                value: *const Cell<f64>,
                _: *const arby::signal_graph::graph::GraphAggregateData,
            ) {
                let ptr = ptr as *mut <Self as _CallSignal_>::Item;
                let value_offset = match index {
                    ConsumerOrAggregate::Consumer(value_offset) => value_offset,
                    ConsumerOrAggregate::Aggregate(_, _) => unsafe {
                        if cfg!(debug_assertsions) {
                            unreachable!()
                        } else {
                            std::hint::unreachable_unchecked();
                        }
                    },
                };
                let offset = value_offset as usize;
                let real_ref = Self::shrink_lifetime_mut(ptr, &offset);
                let parent_value =
                    Self::shrink_lifetime(unsafe { value.offset(value_offset as isize) }, &offset);
                let value = Self::shrink_lifetime(value, &offset);
                value.set($fn_name(real_ref, parent_value.get(), time))
            }
        }

        Signal {
            sig_type: arby::signal_graph::graph_registrar::InputSignalType::Inner(
                false,
                InnerCallback::redirect_call,
            ),
            type_check: InnerCallback::check_type,
        }
    }};
}

#[macro_export]
macro_rules! make_aggregate_callback {
    ($fn_name: ident, $aggregate_type: ty) => {{
        use std::cell::Cell;
        struct InnerCallback {}
        unsafe impl _CallSignal_ for InnerCallback {
            type Item = $aggregate_type;
            const TYPE_NAME: &'static str = stringify!($aggregate_type);
        }
        use arby::signal_graph::graph::ConsumerOrAggregate;
        impl InnerCallback {
            fn redirect_call(
                ptr: *mut u8,
                index: ConsumerOrAggregate,
                time: u64,
                value: *const Cell<f64>,
                aggregate: *const arby::signal_graph::graph::GraphAggregateData,
            ) {
                let ptr = ptr as *mut <Self as _CallSignal_>::Item;
                let (mask, offset) = match index {
                    ConsumerOrAggregate::Aggregate(mask, offset) => (mask, offset),
                    ConsumerOrAggregate::Consumer(_) => {
                        if cfg!(debug_assertsions) {
                            unreachable!()
                        } else {
                            unsafe { std::hint::unreachable_unchecked() };
                        }
                    }
                };
                let offset = offset as usize;
                let real_ref = Self::shrink_lifetime_mut(ptr, &offset);
                let value = Self::shrink_lifetime(value, &offset);
                let graph_agg = Self::shrink_lifetime(aggregate, &offset);
                debug_assert!(graph_agg.aggregate_signals.len() > offset);
                let agg_signal = unsafe { graph_agg.aggregate_signals.get_unchecked(offset) };
                value.set($fn_name(
                    real_ref,
                    arby::signal_graph::interface_types::AggregateUpdate::new(mask, agg_signal),
                    time,
                ))
            }
        }

        Signal {
            sig_type: arby::signal_graph::graph_registrar::InputSignalType::Inner(
                true,
                InnerCallback::redirect_call,
            ),
            type_check: InnerCallback::check_type,
        }
    }};
}

#[macro_export]
macro_rules! make_model_callback {
    ($fn_name: ident, $type: ty) => {{
        use std::cell::Cell;
        struct InnerCallback {}
        unsafe impl _CallSignal_ for InnerCallback {
            type Item = $type;
            const TYPE_NAME: &'static str = stringify!($type);
        }
        impl InnerCallback {
            fn redirect_call(ptr: *mut u8, time: u64) {
                let ptr = ptr as *mut <Self as _CallSignal_>::Item;
                let dummy = 0;
                let value = Self::shrink_lifetime(value, &dummy);
                let real_ref = Self::shrink_lifetime_mut(ptr, &offset);
                $fn_name(real_ref, time)
            }
        }
        Signal {
            sig_type: arby::signal_graph::graph_registrar::InputSignalType::Book(
                InnerCallback::redirect_call,
            ),
            type_check: InnerCallback::check_type,
        }
    }};
}

#[macro_export]
macro_rules! make_book_callback {
    ($fn_name: ident, $type: ty) => {{
        use std::cell::Cell;
        struct InnerCallback {}
        unsafe impl _CallSignal_ for InnerCallback {
            type Item = $type;
            const TYPE_NAME: &'static str = stringify!($type);
        }
        impl InnerCallback {
            fn redirect_call(
                ptr: *mut u8,
                book_update: (*const BookState, BookUpdateMask),
                time: u64,
                value: *const Cell<f64>,
            ) {
                let ptr = ptr as *mut <Self as _CallSignal_>::Item;
                let dummy = 0;
                let (book_ptr, book_mask) = book_update;
                let book_update = BookUpdate {
                    book: Self::shrink_lifetime(book_ptr, &dummy),
                    updated_mask: book_mask,
                };
                let value = Self::shrink_lifetime(value, &dummy);
                let real_ref = Self::shrink_lifetime_mut(ptr, &dummy);
                value.set($fn_name(real_ref, book_update, time))
            }
        }
        Signal {
            sig_type: arby::signal_graph::graph_registrar::InputSignalType::Book(
                InnerCallback::redirect_call,
            ),
            type_check: InnerCallback::check_type,
        }
    }};
}

#[derive(Clone)]
pub enum BookSignalType {
    AllBook,
    Bbo,
}

pub type BookCallback = fn(*mut u8, (*const BookState, BookUpdateMask), u64, *const Cell<f64>);
// All of the non-book signals are made into an into a single callback,
// the consumer ones deconstruct the aggregate signal into a consumer signal
pub type InnerCallback =
    fn(*mut u8, ConsumerOrAggregate, u64, *const Cell<f64>, *const GraphAggregateData);

pub enum InputSignalInfo {
    // TODO add direct book-info references
    BookSignal,
    Consumer(ConsumerSignal),
    Aggregator(Vec<AggregateSignal>),
}
