use super::security_index::Security;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("Multiple signals given to registrar with name {0}")]
    DuplicateSignalName(&'static str),
    #[error("Multiple signal instances given to registrar creation with name {0}")]
    DuplicateSignalInstance(String),
    #[error("Signal definition {definition} requested by {signal} not found")]
    DefinitionNotFound { definition: String, signal: String },
    // TODO test
    #[error("Input {input} not found on signal {signal}")]
    InputNotFound { input: &'static str, signal: String },
    // TODO test
    #[error("Signal {signal} missing subscription for inputs {inputs:?}")]
    MissingSubscription {
        signal: String,
        inputs: Vec<&'static str>
    },
    // TODO test
    #[error("Book input {security:?} not found")]
    BookNotFound { security: Security },
    // TODO test
    #[error("Parent output {parent}:{output} requested by signal {child} input {input} not found")]
    ParentNotFound {
        parent: String,
        output: String,
        input: String,
        child: String,
    },
    // TODO Test
    #[error("Signal instance {instance} has too many entries {entries} in aggregate {input}")]
    AggregateTooLarge {
        instance: String,
        entries: usize,
        input: &'static str,
    },
    #[error("Aggregate {name} on signal {signal} has no inputs")]
    AggregateNoInputs { signal: String, name: &'static str },
    // TODO test
    #[error("Too many signals in graph {0}, maximum is 2^16-1")]
    TooManySignals(usize),
    // TODO test
    #[error("Too many signals in graph {0}, maximum references is 2^16-1. This can be increased")]
    TooManyAggregateReferences(usize),
    //TODO test
    #[error(
        "Cycle discovered in graph call {call} containing {signals:?} in block for {security:?}"
    )]
    GraphCycle {
        call: String,
        signals: Vec<String>,
        security: Security,
    },
}
