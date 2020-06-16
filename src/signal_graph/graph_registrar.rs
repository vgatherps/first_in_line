use super::interface_types::*;
use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::graph::{Graph, GraphCallList, GraphInnerMem};
use super::graph_error::GraphError;
use super::graph_sort::generate_calls_for;
use super::security_data::SecurityVector;
use super::security_index::{Security, SecurityMap};

pub type GraphHandle = GraphInnerMem;

pub struct GraphRegistrar {
    signal_definitions: HashMap<&'static str, SignalDefinition>,
}

#[derive(Clone)]
pub struct SignalDefinition {
    pub(crate) inputs: HashMap<&'static str, SignalType>,
    pub(crate) outputs: HashSet<&'static str>,
    pub(crate) creator: fn(
        outputs: HashMap<&'static str, ConsumerOutput>,
        inputs: InputLoader,
        json: Option<&str>,
        name: &str,
        objects: &mut dynstack::DynStack<dyn CallSignal>,
    ) -> Result<u16, GraphError>,
    pub(crate) caller: fn(obj: *mut u8,time: u128, graph: &GraphInnerMem),
    pub(crate) cleanup: Option<fn(obj: *mut u8,time: u128, graph: &GraphInnerMem)>,
}

#[derive(Clone)]
pub(crate) struct SignalInstantiation {
    pub(crate) definition: SignalDefinition,
    pub(crate) inputs: HashMap<String, NamedSignalType>,
}

#[derive(Copy, Clone, Debug)]
pub enum SignalType {
    Book,
    Consumer,
    Aggregate,
}

#[derive(Clone, Debug)]
pub enum NamedSignalType {
    Book(Security),
    Consumer((String, String)),
    Aggregate(Vec<(String, String)>),
}

// TODO should be called instantiation, name already taken
#[derive(Debug)]
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
        inits: &HashMap<String, String>,
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

        let inner_mem = GraphInnerMem::new(signal_to_instance, layout, security_map, inits)?;
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

        let security_call_list = SecurityVector::new_with_err(security_map, |sec, _| {
            if requested_book_signals.contains(sec) {
                Some(generate_calls_for(sec, inner_mem.clone())).transpose()
            } else {
                Ok(None)
            }
        })?;

        Ok(Graph {
            book_updates: security_call_list,
            mem: inner_mem,
        })
    }
}

pub trait CallSignal {
    fn call_signal(&mut self, time: u128, graph: &GraphInnerMem);
    fn cleanup(&mut self, time: u128, _: &GraphInnerMem) {
        unimplemented!("Cleanup called for signal without implementation, check your registrations")
    }
}

pub fn make_signal_for<T: CallSignal + RegisterSignal<Child = T> + 'static>() -> SignalDefinition {
    fn _real_create<F: CallSignal + RegisterSignal<Child = F> + 'static>(
        outputs: HashMap<&'static str, ConsumerOutput>,
        inputs: InputLoader,
        json: Option<&str>,
        name: &str,
        objects: &mut dynstack::DynStack<dyn CallSignal>,
    ) -> Result<u16, GraphError> {
        if json.is_some() && !F::PARAMS {
            return Err(GraphError::NodeGotParams(name.to_string()));
        }
        if json.is_none() && F::PARAMS {
            return Err(GraphError::NodeNoParams(name.to_string()));
        }
        let val = match F::create(outputs, inputs, json) {
            Ok(val) => val,
            Err(err) => return Err(GraphError::NodeInitError(err)),
        };
        // BOOOOO rust and weird type specification problems
        // make it impossible to do this another way.
        let index = objects.len();
        dynstack::dyn_push!(objects, val);

        assert!(index < std::u16::MAX as usize);
        Ok(index as u16)
    }

    fn _call_signal<F: CallSignal>(data: *mut u8, time: u128, graph: &GraphInnerMem) {
        let real_ref = unsafe { &mut *(data as *mut F) };
        real_ref.call_signal(time, graph)
    }

    fn _cleanup_signal<F: CallSignal>(data: *mut u8, time: u128, graph: &GraphInnerMem) {
        let real_ref = unsafe { &mut *(data as *mut F) };
        real_ref.cleanup(time, graph)
    }

    SignalDefinition {
        inputs: T::get_inputs(),
        outputs: T::get_outputs(),
        creator: _real_create::<T>,
        caller: _call_signal::<T>,
        cleanup: if T::CLEANUP { Some(_cleanup_signal::<T>) } else { None },
    }
}

pub struct InputLoader {
    pub(crate) all_inputs: HashMap<&'static str, Box<dyn Any>>,
}

impl InputLoader {
    pub fn load_input<T: InputType>(&mut self, name: &'static str) -> Result<T, anyhow::Error> {
        if let Some(signal) = self.all_inputs.remove(name) {
            if let Ok(downcast) = signal.downcast::<T>() {
                Ok(*downcast)
            } else {
                anyhow::bail!("Wong type requested on input {}", name);
            }
        } else {
            anyhow::bail!("Could not find signal for input {}", name);
        }
    }
}

pub trait RegisterSignal {
    type Child: CallSignal + 'static;
    const PARAMS: bool = true;
    const CLEANUP: bool = false;
    fn get_inputs() -> HashMap<&'static str, SignalType>;
    fn get_outputs() -> HashSet<&'static str>;
    fn create(
        outputs: HashMap<&'static str, ConsumerOutput>,
        inputs: InputLoader,
        json: Option<&str>,
    ) -> Result<Self::Child, anyhow::Error>;
}
