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
    #[error("Book input {security:?} not found")]
    BookNotFound { security: Security },
    // TODO test
    #[error("Missing subscription in by signal {signal} on names {inputs:?}")]
    MissingSubscription { signal: String, inputs: Vec<String> },
    #[error("Parent {parent} requested by signal {child} input {input} not found")]
    ParentNotFound {
        child: String,
        parent: String,
        input: String,
    },
    // TODO Test
    #[error("Signal instance {instance} has too many entries in aggregate {input}")]
    AggregateTooLarge {
        instance: String,
        input: &'static str,
    },
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
