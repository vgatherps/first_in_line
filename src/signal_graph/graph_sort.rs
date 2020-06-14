use super::interface_types::*;
use std::any::Any;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::graph::{index_to_bitmap, Graph, GraphCallList, GraphInnerMem};
use super::graph_error::GraphError;
use super::graph_registrar::*;
use super::security_data::SecurityVector;
use super::security_index::{Security, SecurityMap};

pub(crate) fn find_seen_signals(
    security: &Security,
    signal_name_to_instance: &HashMap<String, SignalInstantiation>,
) -> Result<HashSet<String>, GraphError> {
    let mut seen_signals: HashSet<String> = HashSet::new();
    // First, find the direct dependencies of the security
    for (signal_name, instance) in signal_name_to_instance {
        let signal_name = signal_name;
        for (input_name, signal_type) in &instance.inputs {
            match signal_type {
                NamedSignalType::Book(sec) if sec == security => {
                    seen_signals.insert(signal_name.clone());
                }
                _ => (),
            }
        }
    }

    // now, keep adding new signals to the map until we find nothing new
    // This is not an efficient algorithm, but it's simple.
    loop {
        let starting_size = seen_signals.len();

        for (signal_name, instance) in signal_name_to_instance {
            let signal_name: &str = signal_name;
            for (input_name, signal_type) in &instance.inputs {
                let parents = match signal_type {
                    NamedSignalType::Consumer(parent) => vec![parent.0.clone()],
                    NamedSignalType::Aggregate(parents) => {
                        parents.iter().map(|(s, n)| s.clone()).collect()
                    }
                    NamedSignalType::Book(_) => vec![],
                };

                // If there are any parents in the set of seen signals
                // we first check if the signal has already been inserted into the seen set
                // If it has, and it's with the same trigger name, then we ignore it.
                // If it has, and this is a new trigger name, then we return an error
                // If it hasn't, we insert it into the seen set
                if parents
                    .into_iter()
                    .filter(|p| seen_signals.contains(p))
                    .next()
                    .is_some()
                {
                    seen_signals.insert(signal_name.to_string());
                }
            }
        }

        if starting_size == seen_signals.len() {
            return Ok(seen_signals);
        }
    }
}

pub(crate) fn topological_sort(
    seen_signals: &HashSet<String>,
    signal_name_to_instance: &HashMap<String, SignalInstantiation>,
) -> Vec<String> {
    let mut dependencies = HashMap::new();
    // gather the dependencies for every signal in the seen_signals set
    for signal in seen_signals {
        let parents: HashSet<_> = signal_name_to_instance
            .get(signal)
            .expect("Missing signal late in process")
            .inputs
            .values()
            .map(|parents| match parents {
                NamedSignalType::Consumer(parent) => vec![parent.0.clone()],
                NamedSignalType::Aggregate(parents) => {
                    parents.iter().map(|(s, n)| s.clone()).collect()
                }
                NamedSignalType::Book(_) => vec![],
            })
            .flat_map(|parents| parents.into_iter())
            .filter(|parent| seen_signals.contains(parent.as_str()))
            .collect();
        dependencies.insert(signal, parents);
    }

    // sort the signals, starting by clearing out the book signals
    let mut ordered_signals = Vec::new();
    loop {
        let mut new_empty_signals = Vec::new();
        let starting_size = ordered_signals.len();

        for (child, parents) in dependencies.iter() {
            if parents.len() == 0 {
                new_empty_signals.push((*child).clone());
            }
        }
        for signal in &new_empty_signals {
            for (child, parents) in dependencies.iter_mut() {
                parents.remove(signal);
            }
        }

        for signal in new_empty_signals {
            dependencies.remove(&signal);
            ordered_signals.push(signal);
        }

        if ordered_signals.len() == starting_size {
            if dependencies.len() != 0 {
                panic!("had a cycle with signals {:?}", dependencies.keys());
            }
            return ordered_signals;
        }
    }
}

pub(crate) fn generate_calls_for(
    security: &Security,
    mem: Rc<GraphInnerMem>,
) -> Result<GraphCallList, GraphError> {
    let seen_signals = find_seen_signals(security, &mem.signal_name_to_instance)?;
    let sorted = topological_sort(&seen_signals, &mem.signal_name_to_instance);

    // Now generate the list of distinct mark indices to mark
    // Since we generate indices in terms of call order, this hopefully should be fairly compact
    // per graph

    let mark_as_clean: HashSet<_> = mem
        .signal_output_to_index
        .iter()
        .map(|((sig, _), index)| (sig, index))
        .filter(|(sig, _)| seen_signals.contains(*sig))
        .map(|(_, index)| {
            let (offset, _) = index_to_bitmap(*index);
            offset
        })
        .collect();

    let mut mark_as_clean: Vec<_> = mark_as_clean.into_iter().collect();
    mark_as_clean.sort();

    // now generate list of actual calls

    let mut calls = Vec::new();
    let mut cleanup = Vec::new();

    for signal in sorted {
        let index = mem
            .signal_name_to_index
            .get(&signal)
            .expect("signal name missing late");
        let inst = mem
            .signal_name_to_instance
            .get(&signal)
            .expect("signal name missing late");
        let object = &mem.objects[*index as usize];
        // this carefully, carefully, carefully works since we control
        // when the actual objects are referenced out of the graph list.
        // This pointer is only dereferenced during the calls, and we won't have
        // overlapping references
        let object_ptr = unsafe { object as *const _ as *mut _ };
        calls.push(object_ptr);
        if inst.definition.cleanup {
            cleanup.push(object_ptr);
        }
    }

    Ok(GraphCallList {
        calls,
        cleanup,
        mark_as_clean,
        mem,
    })
}
