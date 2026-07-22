//! Pipeline condition language (PRD §8.2): a small expression grammar over
//! status / mime / method / headers / variables, parsed and checked at
//! config time — replacing Restspace's ad-hoc JS evaluation.
//!
//! Grammar:
//! ```text
//! expr    := or
//! or      := and ( "||" and )*
//! and     := unary ( "&&" unary )*
//! unary   := "!" unary | cmp
//! cmp     := primary ( ("=="|"!="|"<="|">="|"<"|">") primary )?
//! primary := literal | builtin | "$" var ("." ident)* | header(STR) | "(" expr ")"
//! builtin := status | ok | method | mime | isJson | isText | isBinary | name
//! literal := STRING | NUMBER | true | false | null
//! ```

use serde_json::Value;

use crate::error::RsError;
use crate::message::Message;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    /// A built-in message accessor (`status`, `ok`, `method`, ...).
    Builtin(String),
    /// `$name.path.to.field`
    Var(String, Vec<String>),
    /// `header("x-thing")`
    Header(String),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Cmp(Box<Expr>, CmpOp, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

const BUILTINS: &[&str] = &[
    "status",
    "ok",
    "method",
    "mime",
    "isJson",
    "isText",
    "isBinary",
    "name",
    "isDirectory",
];

#[derive(Debug, Clone)]
pub struct Condition {
    expr: Expr,
    pub source: String,
}

impl Condition {
    pub fn parse(source: &str) -> Result<Condition, String> {
        let mut p = Parser {
            input: source.as_bytes(),
            pos: 0,
        };
        let expr = p.expr()?;
        p.skip_ws();
        if p.pos != p.input.len() {
            return Err(format!("unexpected input at position {}", p.pos));
        }
        Ok(Condition {
            expr,
            source: source.to_string(),
        })
    }

    /// Evaluate against a message and the pipeline's variables.
    pub fn evaluate(
        &self,
        msg: &Message,
        vars: &serde_json::Map<String, Value>,
    ) -> Result<bool, RsError> {
        Ok(truthy(&eval(&self.expr, msg, vars)?))
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

fn eval(
    expr: &Expr,
    msg: &Message,
    vars: &serde_json::Map<String, Value>,
) -> Result<Value, RsError> {
    Ok(match expr {
        Expr::Literal(v) => v.clone(),
        Expr::Builtin(name) => {
            let status = msg.status.map(|s| s.as_u16()).unwrap_or(200);
            let mime = msg
                .body
                .as_ref()
                .map(|b| b.media_type.essence().to_string());
            match name.as_str() {
                "status" => Value::from(status),
                "ok" => Value::Bool(msg.is_ok()),
                "method" => Value::String(msg.method.to_string()),
                "mime" => mime.map(Value::String).unwrap_or(Value::Null),
                "isJson" => Value::Bool(msg.body.as_ref().is_some_and(|b| b.media_type.is_json())),
                "isText" => Value::Bool(msg.body.as_ref().is_some_and(|b| b.media_type.is_text())),
                "isBinary" => Value::Bool(
                    msg.body
                        .as_ref()
                        .is_some_and(|b| !b.media_type.is_json() && !b.media_type.is_text()),
                ),
                "name" => msg.name.clone().map(Value::String).unwrap_or(Value::Null),
                "isDirectory" => Value::Bool(msg.url.is_directory()),
                other => return Err(RsError::internal(format!("unknown builtin '{other}'"))),
            }
        }
        Expr::Var(name, path) => {
            let mut v = vars.get(name).cloned().unwrap_or(Value::Null);
            for key in path {
                v = match v {
                    Value::Object(mut o) => o.remove(key).unwrap_or(Value::Null),
                    _ => Value::Null,
                };
            }
            v
        }
        Expr::Header(name) => msg
            .header(name)
            .map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null),
        Expr::Not(e) => Value::Bool(!truthy(&eval(e, msg, vars)?)),
        Expr::And(l, r) => {
            Value::Bool(truthy(&eval(l, msg, vars)?) && truthy(&eval(r, msg, vars)?))
        }
        Expr::Or(l, r) => Value::Bool(truthy(&eval(l, msg, vars)?) || truthy(&eval(r, msg, vars)?)),
        Expr::Cmp(l, op, r) => {
            let (l, r) = (eval(l, msg, vars)?, eval(r, msg, vars)?);
            Value::Bool(compare(&l, *op, &r))
        }
    })
}

fn compare(l: &Value, op: CmpOp, r: &Value) -> bool {
    use std::cmp::Ordering;
    let ord = match (l, r) {
        (Value::Number(a), Value::Number(b)) => a
            .as_f64()
            .zip(b.as_f64())
            .and_then(|(a, b)| a.partial_cmp(&b)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    };
    match op {
        // Numeric equality compares values (integer 200 == literal 200.0).
        CmpOp::Eq => ord.map(|o| o == Ordering::Equal).unwrap_or(l == r),
        CmpOp::Ne => ord.map(|o| o != Ordering::Equal).unwrap_or(l != r),
        CmpOp::Lt => ord == Some(Ordering::Less),
        CmpOp::Gt => ord == Some(Ordering::Greater),
        CmpOp::Le => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
        CmpOp::Ge => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
    }
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn eat(&mut self, token: &str) -> bool {
        self.skip_ws();
        if self.input[self.pos..].starts_with(token.as_bytes()) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let mut left = self.and()?;
        loop {
            if self.eat("||") {
                let right = self.and()?;
                left = Expr::Or(Box::new(left), Box::new(right));
            } else {
                return Ok(left);
            }
        }
    }

    fn and(&mut self) -> Result<Expr, String> {
        let mut left = self.unary()?;
        loop {
            if self.eat("&&") {
                let right = self.unary()?;
                left = Expr::And(Box::new(left), Box::new(right));
            } else {
                return Ok(left);
            }
        }
    }

    fn unary(&mut self) -> Result<Expr, String> {
        self.skip_ws();
        // `!` but not `!=`
        if self.peek() == Some(b'!') && self.input.get(self.pos + 1) != Some(&b'=') {
            self.pos += 1;
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        self.cmp()
    }

    fn cmp(&mut self) -> Result<Expr, String> {
        let left = self.primary()?;
        self.skip_ws();
        let op = if self.eat("==") {
            CmpOp::Eq
        } else if self.eat("!=") {
            CmpOp::Ne
        } else if self.eat("<=") {
            CmpOp::Le
        } else if self.eat(">=") {
            CmpOp::Ge
        } else if self.eat("<") {
            CmpOp::Lt
        } else if self.eat(">") {
            CmpOp::Gt
        } else {
            return Ok(left);
        };
        let right = self.primary()?;
        Ok(Expr::Cmp(Box::new(left), op, Box::new(right)))
    }

    fn primary(&mut self) -> Result<Expr, String> {
        self.skip_ws();
        match self.peek() {
            None => Err("unexpected end of expression".to_string()),
            Some(b'(') => {
                self.pos += 1;
                let inner = self.expr()?;
                if !self.eat(")") {
                    return Err("expected ')'".to_string());
                }
                Ok(inner)
            }
            Some(b'\'') | Some(b'"') => {
                let quote = self.input[self.pos];
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.input.len() && self.input[self.pos] != quote {
                    self.pos += 1;
                }
                if self.pos >= self.input.len() {
                    return Err("unterminated string".to_string());
                }
                let s = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
                self.pos += 1;
                Ok(Expr::Literal(Value::String(s)))
            }
            Some(b'$') => {
                self.pos += 1;
                let name = self.ident()?;
                let mut path = Vec::new();
                while self.peek() == Some(b'.') {
                    self.pos += 1;
                    path.push(self.ident()?);
                }
                Ok(Expr::Var(name, path))
            }
            Some(c) if c.is_ascii_digit() || c == b'-' => {
                let start = self.pos;
                if c == b'-' {
                    self.pos += 1;
                }
                while self.peek().is_some_and(|c| c.is_ascii_digit() || c == b'.') {
                    self.pos += 1;
                }
                let text = String::from_utf8_lossy(&self.input[start..self.pos]);
                let n: f64 = text.parse().map_err(|_| format!("bad number '{text}'"))?;
                Ok(Expr::Literal(serde_json::json!(n)))
            }
            Some(c) if c.is_ascii_alphabetic() || c == b'_' => {
                let name = self.ident()?;
                match name.as_str() {
                    "true" => Ok(Expr::Literal(Value::Bool(true))),
                    "false" => Ok(Expr::Literal(Value::Bool(false))),
                    "null" => Ok(Expr::Literal(Value::Null)),
                    "header" => {
                        if !self.eat("(") {
                            return Err("header requires '(\"name\")'".to_string());
                        }
                        let arg = self.primary()?;
                        if !self.eat(")") {
                            return Err("expected ')' after header argument".to_string());
                        }
                        match arg {
                            Expr::Literal(Value::String(s)) => Ok(Expr::Header(s)),
                            _ => Err("header argument must be a string literal".to_string()),
                        }
                    }
                    _ if BUILTINS.contains(&name.as_str()) => Ok(Expr::Builtin(name)),
                    other => Err(format!(
                        "unknown identifier '{other}' (builtins: {})",
                        BUILTINS.join(", ")
                    )),
                }
            }
            Some(c) => Err(format!("unexpected character '{}'", c as char)),
        }
    }

    fn ident(&mut self) -> Result<String, String> {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            self.pos += 1;
        }
        if self.pos == start {
            return Err("expected identifier".to_string());
        }
        Ok(String::from_utf8_lossy(&self.input[start..self.pos]).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    fn msg_with_status(status: u16) -> Message {
        let req = Message::request(Method::GET, "/x", "t");
        req.response(http::StatusCode::from_u16(status).unwrap(), None)
    }

    #[test]
    fn parses_and_evaluates_core_forms() {
        let vars = serde_json::json!({ "order": { "status": "open", "total": 5 } });
        let vars = vars.as_object().unwrap();
        let msg = msg_with_status(200);
        for (expr, expected) in [
            ("ok", true),
            ("status == 200", true),
            ("status >= 400", false),
            ("$order.status == 'open'", true),
            ("$order.total > 4 && ok", true),
            ("!ok || $order.status == 'open'", true),
            ("$missing", false),
            ("method == 'GET'", true),
        ] {
            let cond = Condition::parse(expr).unwrap();
            assert_eq!(cond.evaluate(&msg, vars).unwrap(), expected, "{expr}");
        }
    }

    #[test]
    fn header_accessor_and_grouping() {
        let mut msg = msg_with_status(200);
        msg.set_header("x-mode", "manage");
        let vars = serde_json::Map::new();
        let cond = Condition::parse("(header(\"x-mode\") == 'manage') && ok").unwrap();
        assert!(cond.evaluate(&msg, &vars).unwrap());
    }

    #[test]
    fn config_time_rejection_of_bad_grammar() {
        assert!(Condition::parse("status ==").is_err());
        assert!(Condition::parse("unknownIdent == 1").is_err());
        assert!(Condition::parse("status == 200 extra").is_err());
        assert!(Condition::parse("'unterminated").is_err());
    }
}
