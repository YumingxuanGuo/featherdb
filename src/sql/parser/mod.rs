#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

pub mod ast;
mod lexer;
pub use lexer::{Keyword, Lexer, Token};

use crate::error::{Result, Error};


/// An SQL parser
pub struct Parser<'a> {
    lexer: std::iter::Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    /// Creates a new parser for the given string input.
    pub fn new(query: &str) -> Parser {
        Parser { lexer: Lexer::new(query).peekable() }
    }

    /// Parses an SQL query.
    pub fn parse(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Grabs the next lexer token, or throws an error if none is found.
    fn next(&mut self) -> Result<Token> {
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse("unexpected end of input".into())))
    }

    /// Grabs the next lexer token, and returns it if it was expected or otherwise throws an error.
    fn next_expect(&mut self, expected: Option<Token>) -> Result<Option<Token>> {
        match expected {
            Some(token) => {
                let actual = self.next()?;
                if actual == token {
                    Ok(Some(token))
                } else {
                    Err(Error::Parse(format!("Expected token {}, found {}", token, actual)))
                }
            },
            None => {
                if let Some(token) = self.peek()? {
                    Err(Error::Parse(format!("Expected end of tokens, found {}", token)))
                } else {
                    Ok(None)
                }
            },
        }
    }

    /// Grabs the next identifier, or errors if not found.
    fn next_identifier(&mut self) -> Result<String> {
        match self.next()? {
            Token::Identifier(identifier) => Ok(identifier),
            token => Err(Error::Parse(format!("Expected identifier, found {}", token))),
        }
    }

    /// Grabs the next lexer token if it satisfies the predicate function.
    fn next_if<F>(&mut self, predicate: F) -> Option<Token>
    where
        F: Fn(&Token) -> bool,
    {
        self.peek()
            .unwrap_or(None)
            .filter(|token| predicate(token))
            .and_then(|_| self.next().ok())
    }

    /// Grabs the next operator if it satisfies the type and precedence.
    fn next_if_operator<O: Operator>(&mut self, min_prec: u8) -> Result<Option<O>> {
        if let Some(op) = self
            .peek()
            .unwrap_or(None)
            .and_then(|token| O::from(&token))
            .filter(|op| op.precedence() >= min_prec)
        {
            self.next()?;
            Ok(Some(op.augment(self)?))
        } else {
            Ok(None)
        }
    }

    /// Grabs the next lexer token if it is a keyword.
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|token| matches!(token, Token::Keyword(_)))
    }

    /// Grabs the next lexer token if it is a given token.
    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| *t == token)
    }

    /// Peeks the next lexer token if any, but converts it from
    /// Option<Result<Token>> to Result<Option<Token>> which is
    /// more convenient to work with (the Iterator trait requires Option<T>).
    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    /// Parses an SQL statement.
    fn parse_statement(&mut self) -> Result<ast::Statement> {
        match self.peek()? {
            Some(Token::Keyword(Keyword::Begin)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Commit)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Rollback)) => self.parse_transaction(),

            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Drop)) => self.parse_ddl(),

            Some(Token::Keyword(Keyword::Insert)) => self.parse_statement_insert(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_statement_select(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_statement_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_statement_delete(),

            Some(Token::Keyword(Keyword::Explain)) => self.parse_statement_explain(),

            Some(token) => Err(Error::Parse(format!("Unexpected token {}", token))),
            None => Err(Error::Parse("Unexpected end of input".into())),
        }
    }

    /// Parses a transaction statement.
    fn parse_transaction(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses a DDL statement.
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses an INSERT statement.
    fn parse_statement_insert(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses a SELECT statement.
    fn parse_statement_select(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses an UPDATE statement.
    fn parse_statement_update(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses a DELETE statement.
    fn parse_statement_delete(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses an EXPLAIN statement.
    fn parse_statement_explain(&mut self) -> Result<ast::Statement> {
        todo!()
    }
}

/// An operator trait, to help with parsing of operators
trait Operator: Sized {
    /// Looks up the corresponding operator for a token, if one exists
    fn from(token: &Token) -> Option<Self>;
    /// Augments an operator by allowing it to parse any modifiers.
    fn augment(self, parser: &mut Parser) -> Result<Self>;
    /// Returns the operator's associativity
    fn associativity(&self) -> u8;
    /// Returns the operator's precedence
    fn precedence(&self) -> u8;
}