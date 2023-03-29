#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

pub mod ast;
mod lexer;

use crate::error::Result;

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