use crate::error::Result;

pub mod ast;

/// An SQL parser
pub struct Parser {
    
}

impl Parser {
    /// Creates a new parser.
    pub fn new(query: &str) -> Self {
        todo!()
    }

    /// Parses an SQL query.
    pub fn parse(&mut self) -> Result<ast::Statement> {
        todo!()
    }
}