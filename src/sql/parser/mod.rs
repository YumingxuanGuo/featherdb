#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

pub mod ast;
mod lexer;

use lazy_static::lazy_static;
use regex::Regex;
use std::collections::BTreeMap;

pub use lexer::{Keyword, Lexer, Token};

use crate::error::{Result, Error};
use super::types::DataType;


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
        let statement = self.parse_statement()?;
        self.next_if_token(Token::Symbol(lexer::Symbol::Semicolon));
        self.next_expect(None)?;
        Ok(statement)
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
        match self.next()? {
            Token::Keyword(Keyword::Begin) => {
                let mut read_only = false;
                let mut version = None;
                self.next_if_token(Keyword::Transaction.into());
                if self.next_if_token(Keyword::Read.into()).is_some() {
                    match self.next()? {
                        Token::Keyword(Keyword::Only) => read_only = true,
                        Token::Keyword(Keyword::Write) => read_only = false,
                        token => return Err(Error::Parse(format!("Unexpected token {}", token))),
                    }
                }
                if self.next_if_token(Keyword::As.into()).is_some() {
                    self.next_expect(Some(Keyword::Of.into()))?;
                    self.next_expect(Some(Keyword::System.into()))?;
                    self.next_expect(Some(Keyword::Time.into()))?;
                    match self.next()? {
                        Token::Number(n) => version = Some(n.parse::<u64>()?),
                        token => return Err(Error::Parse(format!("Expected number, got {}", token))),
                    }
                }
                Ok(ast::Statement::Begin { read_only, version })
            }

            Token::Keyword(Keyword::Commit) => Ok(ast::Statement::Commit),
            Token::Keyword(Keyword::Rollback) => Ok(ast::Statement::Rollback),
            token => Err(Error::Parse(format!("Unexpected token {}", token))),
        }
    }

    /// Parses a DDL statement.
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("Unexpected token {}", token))),
            },
            Token::Keyword(Keyword::Drop) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_drop_table(),
                token => Err(Error::Parse(format!("Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("Unexpected token {}", token))),
        }
    }

    /// Parses a CREATE TABLE DDL statement. The CREATE TABLE prefix has already been consumed.
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        let name = self.next_identifier()?;
        self.next_expect(Some(Token::Symbol(lexer::Symbol::OpenParen)))?;
        let mut columns = vec![];
        loop {
            columns.push(self.parse_ddl_columnspec()?);
            if self.next_if_token(Token::Symbol(lexer::Symbol::Comma)).is_none() {
                break;
            }
        }
        Ok(ast::Statement::CreateTable { name, columns })
    }

    /// Parses a DROP TABLE DDL statement. The DROP TABLE prefix has already been consumed.
    fn parse_ddl_drop_table(&mut self) -> Result<ast::Statement> {
        Ok(ast::Statement::DropTable(self.next_identifier()?))
    }

    /// Parses a column specification
    fn parse_ddl_columnspec(&mut self) -> Result<ast::Column> {
        let mut column = ast::Column {
            name: self.next_identifier()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Bool) => DataType::Boolean,
                Token::Keyword(Keyword::Boolean) => DataType::Boolean,
                Token::Keyword(Keyword::Char) => DataType::String,
                Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::Float) => DataType::Float,
                Token::Keyword(Keyword::Int) => DataType::Integer,
                Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::String) => DataType::String,
                Token::Keyword(Keyword::Text) => DataType::String,
                Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("Unexpected token {}", token))),
            },
            primary_key: false,
            nullable: None,
            default: None,
            unique: false,
            index: false,
            references: None,
        };
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Primary => {
                    self.next_expect(Some(Keyword::Key.into()))?;
                    column.primary_key = true;
                },
                Keyword::Null => {
                    if let Some(false) = column.nullable {
                        return Err(Error::Value(format!(
                            "Column {} can't be both not nullable and nullable",
                            column.name
                        )));
                    }
                    column.nullable = Some(true)
                },
                Keyword::Not => {
                    self.next_expect(Some(Keyword::Null.into()))?;
                    if let Some(true) = column.nullable {
                        return Err(Error::Value(format!(
                            "Column {} can't be both not nullable and nullable",
                            column.name
                        )));
                    }
                    column.nullable = Some(false)
                },
                Keyword::Default => column.default = Some(self.parse_expression(0)?),
                Keyword::Unique => column.unique = true,
                Keyword::Index => column.index = true,
                Keyword::References => column.references = Some(self.next_identifier()?),
                keyword => return Err(Error::Parse(format!("Unexpected keyword {}", keyword))),
            }
        }
        Ok(column)
    }

    /// Parses an INSERT statement.
    fn parse_statement_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Some(Keyword::Insert.into()))?;
        self.next_expect(Some(Keyword::Into.into()))?;
        let table = self.next_identifier()?;

        let columns = match self.next_if_token(Token::Symbol(lexer::Symbol::OpenParen)) {
            Some(_) => {
                let mut columns = vec![];
                loop {
                    columns.push(self.next_identifier()?);
                    match self.next()? {
                        Token::Symbol(lexer::Symbol::CloseParen) => break,
                        Token::Symbol(lexer::Symbol::Comma) => continue,
                        token => return Err(Error::Parse(format!("Unexpected token {}", token))),
                    }
                }
                Some(columns)
            },
            None => None,
        };

        self.next_expect(Some(Keyword::Values.into()))?;
        let mut values = vec![];
        loop {
            self.next_expect(Some(Token::Symbol(lexer::Symbol::OpenParen)))?;
            let mut expressions = vec![];
            loop {
                expressions.push(self.parse_expression(0)?);
                match self.next()? {
                    Token::Symbol(lexer::Symbol::CloseParen) => break,
                    Token::Symbol(lexer::Symbol::Comma) => continue,
                    token => return Err(Error::Parse(format!("Unexpected token {}", token))),
                }
            }
            values.push(expressions);
            if self.next_if_token(Token::Symbol(lexer::Symbol::Comma)).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Insert { table, columns, values })
    }

    /// TODO: Parses a SELECT statement.
    fn parse_statement_select(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses an UPDATE statement.
    fn parse_statement_update(&mut self) -> Result<ast::Statement> {
        self.next_expect(Some(Keyword::Update.into()))?;
        let table = self.next_identifier()?;
        self.next_expect(Some(Keyword::Set.into()))?;

        let mut set = BTreeMap::new();
        loop {
            let column = self.next_identifier()?;
            self.next_expect(Some(Token::Symbol(lexer::Symbol::Equal)))?;
            let expression = self.parse_expression(0)?;
            if set.contains_key(&column) {
                return Err(Error::Value(format!("Duplicate column {}", column)));
            }
            set.insert(column, expression);
            if self.next_if_token(Token::Symbol(lexer::Symbol::Comma)).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Update { table, set, r#where: self.parse_clause_where()? })
    }

    /// Parses a DELETE statement.
    fn parse_statement_delete(&mut self) -> Result<ast::Statement> {
        self.next_expect(Some(Keyword::Delete.into()))?;
        self.next_expect(Some(Keyword::From.into()))?;
        let table = self.next_identifier()?;
        Ok(ast::Statement::Delete { table, r#where: self.parse_clause_where()? })
    }

    /// TODO: Parses an EXPLAIN statement.
    fn parse_statement_explain(&mut self) -> Result<ast::Statement> {
        todo!()
    }

    /// Parses a WHERE clause.
    fn parse_clause_where(&mut self) -> Result<Option<ast::Expression>> {
        if self.next_if_token(Keyword::Where.into()).is_none() {
            return Ok(None);
        }
        Ok(Some(self.parse_expression(0)?))
    }

    /// Parses an expression consisting of at least one atom operated on by any
    /// number of operators, using the precedence climbing algorithm.
    fn parse_expression(&mut self, min_prec: u8) -> Result<ast::Expression> {
        let mut lhs = if let Some(prefix) = self.next_if_operator::<PrefixOperator>(min_prec)? {
            prefix.build(self.parse_expression(prefix.precedence() + prefix.associativity())?)
        } else {
            self.parse_expression_atom()?
        };
        while let Some(postfix) = self.next_if_operator::<PostfixOperator>(min_prec)? {
            lhs = postfix.build(lhs)
        }
        while let Some(infix) = self.next_if_operator::<InfixOperator>(min_prec)? {
            lhs = infix.build(lhs, self.parse_expression(infix.precedence() + infix.associativity())?)
        }
        Ok(lhs)
    }

    /// Parses an expression atom
    fn parse_expression_atom(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Identifier(id) => {
                if self.next_if_token(Token::Symbol(lexer::Symbol::OpenParen)).is_some() {
                    let mut args = vec![];
                    while self.next_if_token(Token::Symbol(lexer::Symbol::CloseParen)).is_none() {
                        if !args.is_empty() {
                            self.next_expect(Some(Token::Symbol(lexer::Symbol::Comma)))?;
                        }
                        if id == "count" && 
                            self.next_if_token(Token::Symbol(lexer::Symbol::Asterisk)).is_some()
                        {
                            // FIXME: Ugly hack to handle COUNT(*)
                            args.push(ast::Expression::Literal(ast::Literal::Boolean(true)));
                        } else {
                            args.push(self.parse_expression(0)?);
                        }
                    }
                    ast::Expression::Function(id, args)
                } else {
                    let mut relation = None;
                    let mut field = id;
                    if self.next_if_token(Token::Symbol(lexer::Symbol::Period)).is_some() {
                        relation = Some(field);
                        field = self.next_identifier()?;
                    }
                    ast::Expression::Field(relation, field)
                }
            },
            Token::Number(n) => {
                if n.chars().all(|c| c.is_digit(10)) {
                    ast::Literal::Integer(n.parse()?).into()
                } else {
                    ast::Literal::Float(n.parse()?).into()
                }
            },
            Token::Symbol(lexer::Symbol::OpenParen) => {
                let expr = self.parse_expression(0)?;
                self.next_expect(Some(Token::Symbol(lexer::Symbol::CloseParen)))?;
                expr
            },
            Token::String(s) => ast::Literal::String(s).into(),
            Token::Keyword(Keyword::False) => ast::Literal::Boolean(false).into(),
            Token::Keyword(Keyword::Infinity) => ast::Literal::Float(std::f64::INFINITY).into(),
            Token::Keyword(Keyword::NaN) => ast::Literal::Float(std::f64::NAN).into(),
            Token::Keyword(Keyword::Null) => ast::Literal::Null.into(),
            Token::Keyword(Keyword::True) => ast::Literal::Boolean(true).into(),
            t => return Err(Error::Parse(format!("Expected expression atom, found {}", t))),
        })
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

const ASSOC_LEFT: u8 = 1;
const ASSOC_RIGHT: u8 = 0;

/// Prefix operators
enum PrefixOperator {
    Minus,
    Not,
    Plus,
}

impl PrefixOperator {
    fn build(&self, rhs: ast::Expression) -> ast::Expression {
        match self {
            PrefixOperator::Minus => ast::Operation::Negate(Box::new(rhs)).into(),
            PrefixOperator::Not => ast::Operation::Not(Box::new(rhs)).into(),
            PrefixOperator::Plus => ast::Operation::Assert(Box::new(rhs)).into(),
        }
    }
}

impl Operator for PrefixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token {
            Token::Keyword(Keyword::Not) => Some(Self::Not),
            Token::Symbol(lexer::Symbol::Minus) => Some(Self::Minus),
            Token::Symbol(lexer::Symbol::Plus) => Some(Self::Plus),
            _ => None,
        }
    }

    fn augment(self, _parser: &mut Parser) -> Result<Self> {
        Ok(self)
    }

    fn associativity(&self) -> u8 {
        ASSOC_RIGHT
    }

    fn precedence(&self) -> u8 {
        9
    }
}

enum InfixOperator {
    Add,
    And,
    Divide,
    Equal,
    Exponentiate,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    Modulo,
    Multiply,
    NotEqual,
    Or,
    Subtract,
}

impl InfixOperator {
    fn build(&self, lhs: ast::Expression, rhs: ast::Expression) -> ast::Expression {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        match self {
            Self::Add => ast::Operation::Add(lhs, rhs),
            Self::And => ast::Operation::And(lhs, rhs),
            Self::Divide => ast::Operation::Divide(lhs, rhs),
            Self::Equal => ast::Operation::Equal(lhs, rhs),
            Self::Exponentiate => ast::Operation::Exponentiate(lhs, rhs),
            Self::GreaterThan => ast::Operation::GreaterThan(lhs, rhs),
            Self::GreaterThanOrEqual => ast::Operation::GreaterThanOrEqual(lhs, rhs),
            Self::LessThan => ast::Operation::LessThan(lhs, rhs),
            Self::LessThanOrEqual => ast::Operation::LessThanOrEqual(lhs, rhs),
            Self::Like => ast::Operation::Like(lhs, rhs),
            Self::Modulo => ast::Operation::Modulo(lhs, rhs),
            Self::Multiply => ast::Operation::Multiply(lhs, rhs),
            Self::NotEqual => ast::Operation::NotEqual(lhs, rhs),
            Self::Or => ast::Operation::Or(lhs, rhs),
            Self::Subtract => ast::Operation::Subtract(lhs, rhs),
        }
        .into()
    }
}

impl Operator for InfixOperator {
    fn from(token: &Token) -> Option<Self> {
        Some(match token {
            Token::Keyword(Keyword::And) => Self::And,
            Token::Keyword(Keyword::Like) => Self::Like,
            Token::Keyword(Keyword::Or) => Self::Or,
            Token::Symbol(lexer::Symbol::Asterisk) => Self::Multiply,
            Token::Symbol(lexer::Symbol::Caret) => Self::Exponentiate,
            Token::Symbol(lexer::Symbol::Equal) => Self::Equal,
            Token::Symbol(lexer::Symbol::GreaterThan) => Self::GreaterThan,
            Token::Symbol(lexer::Symbol::GreaterThanOrEqual) => Self::GreaterThanOrEqual,
            Token::Symbol(lexer::Symbol::LessOrGreaterThan) => Self::NotEqual,
            Token::Symbol(lexer::Symbol::LessThan) => Self::LessThan,
            Token::Symbol(lexer::Symbol::LessThanOrEqual) => Self::LessThanOrEqual,
            Token::Symbol(lexer::Symbol::Minus) => Self::Subtract,
            Token::Symbol(lexer::Symbol::NotEqual) => Self::NotEqual,
            Token::Symbol(lexer::Symbol::Percent) => Self::Modulo,
            Token::Symbol(lexer::Symbol::Plus) => Self::Add,
            Token::Symbol(lexer::Symbol::Slash) => Self::Divide,
            _ => return None,
        })
    }

    fn augment(self, _parser: &mut Parser) -> Result<Self> {
        Ok(self)
    }

    fn associativity(&self) -> u8 {
        match self {
            Self::Exponentiate => ASSOC_RIGHT,
            _ => ASSOC_LEFT,
        }
    }

    fn precedence(&self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Equal | Self::NotEqual | Self::Like => 3,
            Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual => 4,
            Self::Add | Self::Subtract => 5,
            Self::Multiply | Self::Divide | Self::Modulo => 6,
            Self::Exponentiate => 7,
        }
    }
}

enum PostfixOperator {
    Factorial,
    // FIXME Compiler bug? Why is this considered dead code?
    #[allow(dead_code)]
    IsNull {
        not: bool,
    },
}

impl PostfixOperator {
    fn build(&self, lhs: ast::Expression) -> ast::Expression {
        let lhs = Box::new(lhs);
        match self {
            Self::IsNull { not } => match not {
                true => ast::Operation::Not(Box::new(ast::Operation::IsNull(lhs).into())),
                false => ast::Operation::IsNull(lhs),
            },
            Self::Factorial => ast::Operation::Factorial(lhs),
        }
        .into()
    }
}

impl Operator for PostfixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token {
            Token::Symbol(lexer::Symbol::Exclamation) => Some(Self::Factorial),
            Token::Keyword(Keyword::Is) => Some(Self::IsNull { not: false }),
            _ => None,
        }
    }

    fn augment(mut self, parser: &mut Parser) -> Result<Self> {
        #[allow(clippy::single_match)]
        match &mut self {
            Self::IsNull { ref mut not } => {
                if parser.next_if_token(Keyword::Not.into()).is_some() {
                    *not = true
                };
                parser.next_expect(Some(Keyword::Null.into()))?;
            }
            _ => {}
        };
        Ok(self)
    }

    fn associativity(&self) -> u8 {
        ASSOC_LEFT
    }

    fn precedence(&self) -> u8 {
        8
    }
}

// Formats an identifier by quoting it as appropriate
pub(super) fn format_ident(ident: &str) -> String {
    lazy_static! {
        static ref RE_IDENT: Regex = Regex::new(r#"^\w[\w_]*$"#).unwrap();
    }

    if RE_IDENT.is_match(ident) && Keyword::from_str(ident).is_none() {
        ident.to_string()
    } else {
        format!("\"{}\"", ident.replace("\"", "\"\""))
    }
}
