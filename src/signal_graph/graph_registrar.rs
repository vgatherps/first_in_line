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
        outputs: HashMap<&'static str, ConsumerOutput>,
        input: HashMap<&'static str, ConsumerSignal>,
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
    type ParamType;
    type Child;

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
        outputs: HashMap<&'static str, ConsumerOutput>,
        consumers: HashMap<&'static str, ConsumerSignal>,
        aggregators: HashMap<&'static str, Vec<AggregateSignal>>,
        objects: &mut dynstack::DynStack<dyn Any>,
    ) -> u16 {
        let val = Self::create(consumers, aggregators);
        let index = objects.len();
        dynstack::dyn_push!(objects, val);
        let val = &objects[index];
        assert!(index < std::u16::MAX as usize);
        index as u16
    }
}

// TODO allow storing book references, eliminate the distinction here

pub trait BookCall {
    fn call(&mut self, book: &BookUpdate);
}

pub trait ConsumerCall {
    fn call(&mut self);
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
