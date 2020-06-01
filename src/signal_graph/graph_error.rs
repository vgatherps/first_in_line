use super::security_index::Security;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    // TODO Test
    #[error("Multiple signals given to registrar with name {0}")]
    DuplicateSignalName(&'static str),
    //TODO Test
    #[error("Multiple signal instances given to registrar creation with name {0}")]
    DuplicateSignalInstance(String),
    //TODO test
    #[error("Signal definition {definition} requested by {signal} not found")]
    DefinitionNotFound { definition: String, signal: String },
    // TODO Test
    #[error("Input {input} not found on signal {signal}")]
    InputNotFound { input: &'static str, signal: String },
    #[error("Book input {security:?} not found")]
    BookNotFound { security: Security },
    #[error("Multiple subscription in by signal {signal} on names {name1} and {name2} in block for {security:?}")]
    MultipleSubscription {
        signal: String,
        name1: String,
        name2: String,
        security: Security,
    },
    //TODO Test
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
    #[error("Too many signals in graph {0}")]
    TooManySignals(usize),
    //TODO test
    #[error("Cycle discovered in graph call {call} containing {signals:?}")]
    GraphCycle { call: String, signals: Vec<String> },
}
