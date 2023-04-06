#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::iter::Peekable;
use std::str::Chars;

use crate::error::{Error, Result};

// A lexer token
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    Number(String),
    String(String),
    Identifier(String),
    Keyword(Keyword),
    Symbol(Symbol),
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Token::Number(n) => n,
            Token::String(s) => s,
            Token::Identifier(s) => s,
            Token::Keyword(k) => k.to_str(),
            Token::Symbol(s) => s.to_str(),
        })
    }
}

impl From<Keyword> for Token {
    fn from(keyword: Keyword) -> Self {
        Self::Keyword(keyword)
    }
}

/// Lexer keywords
#[derive(Clone, Debug, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
    Char,
    Commit,
    Create,
    Cross,
    Default,
    Delete,
    Desc,
    Double,
    Drop,
    Explain,
    False,
    Float,
    From,
    Group,
    Having,
    Index,
    Infinity,
    Inner,
    Insert,
    Int,
    Integer,
    Into,
    Is,
    Join,
    Key,
    Left,
    Like,
    Limit,
    NaN,
    Not,
    Null,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Outer,
    Primary,
    Read,
    References,
    Right,
    Rollback,
    Select,
    Set,
    String,
    System,
    Table,
    Text,
    Time,
    Transaction,
    True,
    Unique,
    Update,
    Values,
    Varchar,
    Where,
    Write,
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "AS" => Self::As,
            "ASC" => Self::Asc,
            "AND" => Self::And,
            "BEGIN" => Self::Begin,
            "BOOL" => Self::Bool,
            "BOOLEAN" => Self::Boolean,
            "BY" => Self::By,
            "CHAR" => Self::Char,
            "COMMIT" => Self::Commit,
            "CREATE" => Self::Create,
            "CROSS" => Self::Cross,
            "DEFAULT" => Self::Default,
            "DELETE" => Self::Delete,
            "DESC" => Self::Desc,
            "DOUBLE" => Self::Double,
            "DROP" => Self::Drop,
            "EXPLAIN" => Self::Explain,
            "FALSE" => Self::False,
            "FLOAT" => Self::Float,
            "FROM" => Self::From,
            "GROUP" => Self::Group,
            "HAVING" => Self::Having,
            "INDEX" => Self::Index,
            "INFINITY" => Self::Infinity,
            "INNER" => Self::Inner,
            "INSERT" => Self::Insert,
            "INT" => Self::Int,
            "INTEGER" => Self::Integer,
            "INTO" => Self::Into,
            "IS" => Self::Is,
            "JOIN" => Self::Join,
            "KEY" => Self::Key,
            "LEFT" => Self::Left,
            "LIKE" => Self::Like,
            "LIMIT" => Self::Limit,
            "NAN" => Self::NaN,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "OF" => Self::Of,
            "OFFSET" => Self::Offset,
            "ON" => Self::On,
            "ONLY" => Self::Only,
            "OR" => Self::Or,
            "ORDER" => Self::Order,
            "OUTER" => Self::Outer,
            "PRIMARY" => Self::Primary,
            "READ" => Self::Read,
            "REFERENCES" => Self::References,
            "RIGHT" => Self::Right,
            "ROLLBACK" => Self::Rollback,
            "SELECT" => Self::Select,
            "SET" => Self::Set,
            "STRING" => Self::String,
            "SYSTEM" => Self::System,
            "TABLE" => Self::Table,
            "TEXT" => Self::Text,
            "TIME" => Self::Time,
            "TRANSACTION" => Self::Transaction,
            "TRUE" => Self::True,
            "UNIQUE" => Self::Unique,
            "UPDATE" => Self::Update,
            "VALUES" => Self::Values,
            "VARCHAR" => Self::Varchar,
            "WHERE" => Self::Where,
            "WRITE" => Self::Write,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::And => "AND",
            Self::Begin => "BEGIN",
            Self::Bool => "BOOL",
            Self::Boolean => "BOOLEAN",
            Self::By => "BY",
            Self::Char => "CHAR",
            Self::Commit => "COMMIT",
            Self::Create => "CREATE",
            Self::Cross => "CROSS",
            Self::Default => "DEFAULT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Double => "DOUBLE",
            Self::Drop => "DROP",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::Float => "FLOAT",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::Index => "INDEX",
            Self::Infinity => "INFINITY",
            Self::Inner => "INNER",
            Self::Insert => "INSERT",
            Self::Int => "INT",
            Self::Integer => "INTEGER",
            Self::Into => "INTO",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Key => "KEY",
            Self::Left => "LEFT",
            Self::Like => "LIKE",
            Self::Limit => "LIMIT",
            Self::NaN => "NAN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Of => "OF",
            Self::Offset => "OFFSET",
            Self::On => "ON",
            Self::Only => "ONLY",
            Self::Outer => "OUTER",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Primary => "PRIMARY",
            Self::Read => "READ",
            Self::References => "REFERENCES",
            Self::Right => "RIGHT",
            Self::Rollback => "ROLLBACK",
            Self::Select => "SELECT",
            Self::Set => "SET",
            Self::String => "STRING",
            Self::System => "SYSTEM",
            Self::Table => "TABLE",
            Self::Text => "TEXT",
            Self::Time => "TIME",
            Self::Transaction => "TRANSACTION",
            Self::True => "TRUE",
            Self::Unique => "UNIQUE",
            Self::Update => "UPDATE",
            Self::Values => "VALUES",
            Self::Varchar => "VARCHAR",
            Self::Where => "WHERE",
            Self::Write => "WRITE",
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

/// Lexer symbols
#[derive(Clone, Debug, PartialEq)]
pub enum Symbol {
    Period,
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    LessOrGreaterThan,
    Plus,
    Minus,
    Asterisk,
    Slash,
    Caret,
    Percent,
    Exclamation,
    NotEqual,
    Question,
    OpenParen,
    CloseParen,
    Comma,
    Semicolon,
}

impl Symbol {
    pub fn to_str(&self) -> &str {
        match self {
            Self::Period => ".",
            Self::Equal => "=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::LessOrGreaterThan => "<>",
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Asterisk => "*",
            Self::Slash => "/",
            Self::Caret => "^",
            Self::Percent => "%",
            Self::Exclamation => "!",
            Self::NotEqual => "!=",
            Self::Question => "?",
            Self::OpenParen => "(",
            Self::CloseParen => ")",
            Self::Comma => ",",
            Self::Semicolon => ";",
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

/// A lexer that tokenizes an input string as an iterator.
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self.iter.peek().map(|c| Err(Error::Parse(format!("Unexpected character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given input string.
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer { iter: input.chars().peekable() }
    }

    /// Consumes any whitespace characters.
    fn consume_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    /// Grabs the next character if it matches the given predicate.
    fn next_if<F>(&mut self, predicate: F) -> Option<char>
    where
        F: Fn(char) -> bool,
    {
        self.iter.peek().filter(|&&c| predicate(c))?;
        self.iter.next()
    }

    /// Grabs the next single-character token if the tokenizer function returns one.
    fn next_if_token<F>(&mut self, tokenizer: F) -> Option<Token>
    where
        F: Fn(char) -> Option<Token>,
    {
        let token = self.iter.peek().and_then(|&c| tokenizer(c))?;
        self.iter.next();
        Some(token)
    }

    /// Grabs the next characters that match the predicate, as a string.
    fn next_while<F>(&mut self, predicate: F) -> Option<String>
    where
        F: Fn(char) -> bool,
    {
        let mut str = String::new();
        while let Some(c) = self.next_if(&predicate) {
            str.push(c);
        }
        Some(str).filter(|s| !s.is_empty())
    }

    /// Scans the input for the next token if any, ignoring leading whitespace.
    fn scan(&mut self) -> Result<Option<Token>> {
        self.consume_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string(),
            Some('"') => self.scan_identifier_quoted(),
            Some(c) if c.is_digit(10) => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_word()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    /// Scans the input for the next ident or keyword token, if any.
    fn scan_word(&mut self) -> Option<Token> {
        let mut name = self.next_if(|c| c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            name.push(c);            
        }
        Keyword::from_str(&name)
            .map(Token::Keyword)
            .or_else(|| Some(Token::Identifier(name)))
    }

    /// Scans the input for the next quoted ident, if any.
    fn scan_identifier_quoted(&mut self) -> Result<Option<Token>> {
        if self.next_if(|c| c == '"').is_none() {
            return Ok(None);
        }
        let mut identifier = String::new();
        loop {
            match self.iter.next() {
                Some('"') if self.next_if(|c| c == '"').is_some() => identifier.push('"'),
                Some('"') => break,
                Some(c) => identifier.push(c),
                None => return Err(Error::Parse("Unexpected end of quoted identifier".into())),
            }
        }
        Ok(Some(Token::Identifier(identifier)))
    }

    /// Scans the input for the next number token, if any.
    fn scan_number(&mut self) -> Option<Token> {
        let mut number = self.next_while(|c| c.is_digit(10))?;
        if let Some('.') =  self.next_if(|c| c == '.') {
            number.push('.');
            number.push_str(&self.next_while(|c| c.is_digit(10))?);
        }
        if let Some(exp) = self.next_if(|c| c == 'e' || c == 'E') {
            number.push(exp);
            if let Some(sign) = self.next_if(|c| c == '+' || c == '-') {
                number.push(sign);
            }
            number.push_str(&self.next_while(|c| c.is_digit(10))?);
        }
        Some(Token::Number(number))
    }

    /// Scans the input for the next string literal, if any.
    fn scan_string(&mut self) -> Result<Option<Token>> {
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }
        let mut str = String::new();
        loop {
            match self.iter.next() {
                Some('\'') if self.next_if(|c| c == '\'').is_some() => str.push('\''),
                Some('\'') => break,
                Some(c) => str.push(c),
                None => return Err(Error::Parse("Unexpected end of string literal".into())),
            }
        }
        Ok(Some(Token::String(str)))
    }

    /// Scans the input for the next symbol token, if any, and handle any multi-symbol tokens.
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '.' => Some(Token::Symbol(Symbol::Period)),
            '=' => Some(Token::Symbol(Symbol::Equal)),
            '>' => Some(Token::Symbol(Symbol::GreaterThan)),
            '<' => Some(Token::Symbol(Symbol::LessThan)),
            '+' => Some(Token::Symbol(Symbol::Plus)),
            '-' => Some(Token::Symbol(Symbol::Minus)),
            '*' => Some(Token::Symbol(Symbol::Asterisk)),
            '/' => Some(Token::Symbol(Symbol::Slash)),
            '^' => Some(Token::Symbol(Symbol::Caret)),
            '%' => Some(Token::Symbol(Symbol::Percent)),
            '!' => Some(Token::Symbol(Symbol::Exclamation)),
            '?' => Some(Token::Symbol(Symbol::Question)),
            '(' => Some(Token::Symbol(Symbol::OpenParen)),
            ')' => Some(Token::Symbol(Symbol::CloseParen)),
            ',' => Some(Token::Symbol(Symbol::Comma)),
            ';' => Some(Token::Symbol(Symbol::Semicolon)),
            _ => None,
        })
        .map(|token| match token {
            Token::Symbol(Symbol::Exclamation) if self.next_if(|c| c == '=').is_some() => {
                Token::Symbol(Symbol::NotEqual)
            },
            Token::Symbol(Symbol::GreaterThan) if self.next_if(|c| c == '=').is_some() => {
                Token::Symbol(Symbol::GreaterThanOrEqual)
            },
            Token::Symbol(Symbol::LessThan) if self.next_if(|c| c == '=').is_some() => {
                Token::Symbol(Symbol::LessThanOrEqual)
            },
            Token::Symbol(Symbol::LessThan) if self.next_if(|c| c == '>').is_some() => {
                Token::Symbol(Symbol::LessOrGreaterThan)
            },
            _ => token,
        })
    }
}