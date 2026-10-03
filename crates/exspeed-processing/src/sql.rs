//! ExQL statement parsing.
//!
//! Plain queries go to DataFusion unchanged. ExQL's own statements and
//! clauses are recognised on the token stream (sqlparser's tokenizer, so
//! string literals, quoted identifiers and Unicode are handled correctly)
//! and stripped before the remaining `SELECT` is handed to DataFusion:
//!
//! ```text
//! CREATE [OR REPLACE] STREAM|VIEW <name> AS <select>
//! CREATE [OR REPLACE] TABLE|MATERIALIZED VIEW <name> AS <select>
//! DROP STREAM|VIEW|TABLE|MATERIALIZED VIEW [IF EXISTS] <name>
//! PAUSE QUERY <id> | RESUME QUERY <id> | DROP QUERY <id>
//!
//! <select> extensions (top level only):
//!   FROM <stream> [[AS] alias] [TIMESTAMP BY <expr>]
//!   JOIN <stream> [[AS] alias] [TIMESTAMP BY <expr>] [WITHIN <interval>] ON … [WITHIN <interval>]
//!   WINDOW TUMBLING (SIZE <interval> [, GRACE PERIOD <interval>])
//!   WINDOW HOPPING (SIZE <interval>, ADVANCE BY <interval> [, GRACE PERIOD <interval>])
//!   GRACE PERIOD <interval>
//!   EMIT CHANGES | EMIT FINAL           (last)
//!
//! <interval>: INTERVAL '5 minutes' | '5 minutes' | 5 MINUTES | '1h30m' | '500ms'
//! ```

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use exspeed_common::StreamName;

use crate::error::ExqlError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Emit {
    Changes,
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowSpec {
    Tumbling { size_ms: i64 },
    Hopping { size_ms: i64, advance_ms: i64 },
}

impl WindowSpec {
    /// The windows `[start, end)` containing event time `t` (ms).
    pub fn assign(&self, t: i64) -> Vec<(i64, i64)> {
        match *self {
            WindowSpec::Tumbling { size_ms } => {
                let s = t.div_euclid(size_ms) * size_ms;
                vec![(s, s + size_ms)]
            }
            WindowSpec::Hopping {
                size_ms,
                advance_ms,
            } => {
                // Every start s = k*advance with s <= t < s + size.
                let last = t.div_euclid(advance_ms) * advance_ms;
                let mut out = Vec::new();
                let mut s = last;
                while s > t - size_ms {
                    out.push((s, s + size_ms));
                    s -= advance_ms;
                }
                out.reverse();
                out
            }
        }
    }
}

/// `TIMESTAMP BY` on one relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampBy {
    /// Alias if given, else the table name (lower-cased unless quoted).
    pub relation: Option<String>,
    pub expr_sql: String,
}

/// A relation named after FROM / JOIN at the top level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub name: String,
    pub alias: Option<String>,
}

/// The query part of `CREATE STREAM/TABLE … AS`, with ExQL clauses parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuerySpec {
    pub select_sql: String,
    pub emit: Option<Emit>,
    pub window: Option<WindowSpec>,
    pub grace_ms: Option<i64>,
    pub timestamp_by: Vec<TimestampBy>,
    /// `WITHIN` per JOIN, in the order the JOINs appear.
    pub within: Vec<Option<i64>>,
    pub relations: Vec<Relation>,
}

impl QuerySpec {
    fn has_extensions(&self) -> bool {
        self.emit.is_some()
            || self.window.is_some()
            || self.grace_ms.is_some()
            || !self.timestamp_by.is_empty()
            || self.within.iter().any(|w| w.is_some())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateKind {
    Stream,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateQuery {
    pub kind: CreateKind,
    pub name: String,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub query: QuerySpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// A bounded query (SELECT, WITH, EXPLAIN, …) for DataFusion.
    Query(String),
    Create(CreateQuery),
    Drop {
        kind: CreateKind,
        name: String,
        if_exists: bool,
    },
    PauseQuery(String),
    ResumeQuery(String),
    DropQuery(String),
}

fn is_ws(t: &Token) -> bool {
    matches!(t, Token::Whitespace(_))
}

fn word(t: &Token) -> Option<&Word> {
    match t {
        Token::Word(w) => Some(w),
        _ => None,
    }
}

/// Unquoted keyword match.
fn kw(t: &Token, k: &str) -> bool {
    word(t).is_some_and(|w| w.quote_style.is_none() && w.value.eq_ignore_ascii_case(k))
}

fn ident_value(w: &Word) -> String {
    if w.quote_style.is_some() {
        w.value.clone()
    } else {
        w.value.to_ascii_lowercase()
    }
}


/// Render a token as SQL (sqlparser's `Display` does not re-escape quotes).
fn token_sql(t: &Token) -> String {
    match t {
        Token::SingleQuotedString(s) => format!("'{}'", s.replace('\'', "''")),
        Token::DoubleQuotedString(s) => format!("\"{}\"", s.replace('"', "\"\"")),
        Token::Word(w) => match w.quote_style {
            Some('"') => format!("\"{}\"", w.value.replace('"', "\"\"")),
            Some('`') => format!("`{}`", w.value.replace('`', "``")),
            Some('[') => format!("[{}]", w.value),
            _ => t.to_string(),
        },
        _ => t.to_string(),
    }
}

fn render(tokens: &[Token]) -> String {
    tokens.iter().map(token_sql).collect::<String>().trim().to_string()
}

fn is_arrow(t: &Token) -> bool {
    matches!(t, Token::Arrow | Token::LongArrow)
}

const NOT_A_FUNCTION: &[&str] = &[
    "SELECT", "WHERE", "AND", "OR", "NOT", "ON", "BY", "WHEN", "THEN", "ELSE", "CASE", "IN",
    "AS", "HAVING", "FROM", "JOIN", "IS", "LIKE", "ILIKE", "BETWEEN", "DISTINCT", "EXISTS",
    "ANY", "ALL", "SOME", "VALUES", "USING", "WITH", "RETURNING", "END",
];

fn prev_nonws(v: &[Token], before: usize) -> Option<usize> {
    let mut i = before;
    while i > 0 {
        i -= 1;
        if !is_ws(&v[i]) {
            return Some(i);
        }
    }
    None
}

/// Start index (in `out`) of the operand ending at `k`.
fn left_operand_start(out: &[Token], k: usize) -> Option<usize> {
    match &out[k] {
        Token::RParen => {
            let mut depth = 0i64;
            let mut m = k;
            loop {
                match out[m] {
                    Token::RParen => depth += 1,
                    Token::LParen => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                if m == 0 {
                    return None;
                }
                m -= 1;
            }
            // Function call: name immediately before '('.
            if m > 0 {
                if let Token::Word(w) = &out[m - 1] {
                    let clause = w.quote_style.is_none()
                        && NOT_A_FUNCTION.iter().any(|x| w.value.eq_ignore_ascii_case(x));
                    if !clause {
                        return Some(qualified_start(out, m - 1));
                    }
                }
            }
            Some(m)
        }
        Token::Word(_) => Some(qualified_start(out, k)),
        Token::SingleQuotedString(_) => Some(k),
        _ => None,
    }
}

fn qualified_start(out: &[Token], k: usize) -> usize {
    let mut start = k;
    while start >= 2
        && matches!(out[start - 1], Token::Period)
        && matches!(out[start - 2], Token::Word(_))
    {
        start -= 2;
    }
    start
}

/// End index (inclusive, in `t`) of the operand starting at `j`.
fn right_operand_end(t: &[Token], j: usize) -> Option<usize> {
    match &t[j] {
        Token::SingleQuotedString(_) | Token::Number(_, _) => Some(j),
        Token::LParen => matching_paren(t, j),
        Token::Word(_) => {
            let mut end = j;
            while end + 2 < t.len()
                && matches!(t[end + 1], Token::Period)
                && matches!(t[end + 2], Token::Word(_))
            {
                end += 2;
            }
            if end + 1 < t.len() && matches!(t[end + 1], Token::LParen) {
                return matching_paren(t, end + 1);
            }
            Some(end)
        }
        _ => None,
    }
}

fn matching_paren(t: &[Token], open: usize) -> Option<usize> {
    let mut depth = 0i64;
    for (i, tok) in t.iter().enumerate().skip(open) {
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Parenthesize `a->'k'` / `a->>'k'` so the JSON operators bind tighter
/// than comparison, `IS`, arithmetic and `||` (sqlparser's generic dialect
/// gives them the lowest precedence).
fn wrap_arrows(t: &[Token]) -> Vec<Token> {
    let mut out: Vec<Token> = Vec::with_capacity(t.len() + 8);
    let mut i = 0;
    while i < t.len() {
        if is_arrow(&t[i]) {
            let left = prev_nonws(&out, out.len()).and_then(|k| left_operand_start(&out, k));
            let mut j = i + 1;
            while j < t.len() && is_ws(&t[j]) {
                j += 1;
            }
            let right = if j < t.len() { right_operand_end(t, j) } else { None };
            if let (Some(start), Some(end)) = (left, right) {
                out.insert(start, Token::LParen);
                out.extend_from_slice(&t[i..=end]);
                out.push(Token::RParen);
                i = end + 1;
                continue;
            }
        }
        out.push(t[i].clone());
        i += 1;
    }
    out
}

/// Normalise a SQL fragment: JSON operator precedence, re-escaped literals.
pub fn normalize_sql(sql: &str) -> Result<String, ExqlError> {
    let t = Toks::new(sql)?;
    Ok(render(&wrap_arrows(&t.t)))
}

struct Toks {
    t: Vec<Token>,
}

impl Toks {
    fn new(sql: &str) -> Result<Self, ExqlError> {
        let dialect = GenericDialect {};
        let mut t = Tokenizer::new(&dialect, sql)
            .tokenize()
            .map_err(|e| ExqlError::parse(e.to_string()))?;
        // Drop trailing semicolons / whitespace.
        while matches!(t.last(), Some(Token::SemiColon) | Some(Token::Whitespace(_))) {
            t.pop();
        }
        if t.iter().any(|x| matches!(x, Token::SemiColon)) {
            return Err(ExqlError::parse("only one statement per request is allowed"));
        }
        Ok(Self { t })
    }

    fn len(&self) -> usize {
        self.t.len()
    }

    /// Index of the first non-whitespace token at or after `i`.
    fn nonws(&self, mut i: usize) -> Option<usize> {
        while i < self.t.len() {
            if !is_ws(&self.t[i]) {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    fn get(&self, i: Option<usize>) -> Option<&Token> {
        i.and_then(|i| self.t.get(i))
    }

    fn is_kw(&self, i: Option<usize>, k: &str) -> bool {
        self.get(i).is_some_and(|t| kw(t, k))
    }

    fn text(&self, from: usize, to: usize) -> String {
        render(&wrap_arrows(&self.t[from..to.min(self.t.len())]))
    }
}

/// Parse a duration string such as `5 minutes`, `1h30m`, `500 ms`.
pub fn parse_duration_ms(s: &str) -> Result<i64, ExqlError> {
    let err = || ExqlError::parse(format!("invalid interval '{s}'"));
    let chars: Vec<char> = s.trim().chars().collect();
    let mut i = 0;
    let mut total: f64 = 0.0;
    let mut pairs = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let start = i;
        while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
            i += 1;
        }
        let num: f64 = chars[start..i]
            .iter()
            .collect::<String>()
            .parse()
            .map_err(|_| err())?;
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let ustart = i;
        while i < chars.len() && chars[i].is_ascii_alphabetic() {
            i += 1;
        }
        let unit: String = chars[ustart..i].iter().collect::<String>().to_ascii_lowercase();
        let mult = unit_ms(&unit).ok_or_else(err)?;
        total += num * mult as f64;
        pairs += 1;
    }
    if pairs == 0 || !total.is_finite() || total > 1e15 {
        return Err(err());
    }
    Ok(total.round() as i64)
}

fn unit_ms(unit: &str) -> Option<i64> {
    Some(match unit {
        "ms" | "msec" | "millisecond" | "milliseconds" => 1,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        "w" | "week" | "weeks" => 7 * 86_400_000,
        _ => return None,
    })
}

/// Parse an interval at token `i` (non-ws). Returns (ms, index after).
fn parse_interval(t: &Toks, i: Option<usize>) -> Result<(i64, usize), ExqlError> {
    let missing = || ExqlError::parse("expected an interval (e.g. INTERVAL '5 minutes')");
    let i = i.ok_or_else(missing)?;
    let mut j = i;
    if kw(&t.t[j], "INTERVAL") {
        j = t.nonws(j + 1).ok_or_else(missing)?;
    }
    match &t.t[j] {
        Token::SingleQuotedString(s) => {
            // INTERVAL '10' MINUTE
            if let Ok(n) = s.trim().parse::<f64>() {
                let u = t.nonws(j + 1);
                if let Some(Token::Word(w)) = t.get(u) {
                    if let Some(m) = unit_ms(&w.value.to_ascii_lowercase()) {
                        return Ok(((n * m as f64).round() as i64, u.unwrap() + 1));
                    }
                }
            }
            Ok((parse_duration_ms(s)?, j + 1))
        }
        Token::Number(n, _) => {
            let u = t.nonws(j + 1);
            let unit = match t.get(u) {
                Some(Token::Word(w)) => w.value.to_ascii_lowercase(),
                _ => return Err(missing()),
            };
            let ms = parse_duration_ms(&format!("{n} {unit}"))?;
            Ok((ms, u.unwrap() + 1))
        }
        _ => Err(missing()),
    }
}

const RELATION_STOP: &[&str] = &[
    "ON", "USING", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL",
    "GROUP", "HAVING", "ORDER", "LIMIT", "WINDOW", "TIMESTAMP", "WITHIN", "EMIT", "GRACE",
    "UNION", "OFFSET", "OUTER", "EXCEPT", "INTERSECT",
];

const TS_EXPR_STOP: &[&str] = &[
    "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "ON", "USING", "WITHIN",
    "WHERE", "GROUP", "HAVING", "WINDOW", "EMIT", "GRACE", "ORDER", "LIMIT", "UNION",
];

/// Look ahead from `i` (just after FROM/JOIN/`,`) for `name [[AS] alias]`.
fn peek_relation(t: &Toks, i: usize) -> Option<Relation> {
    let j = t.nonws(i)?;
    let w = word(&t.t[j])?;
    let mut name = ident_value(w);
    let mut k = j + 1;
    // dotted names: a.b.c
    loop {
        let p = t.nonws(k);
        if matches!(t.get(p), Some(Token::Period)) {
            let q = t.nonws(p.unwrap() + 1)?;
            name.push('.');
            name.push_str(&ident_value(word(&t.t[q])?));
            k = q + 1;
        } else {
            break;
        }
    }
    let n = t.nonws(k);
    let alias = match t.get(n) {
        Some(tok) if kw(tok, "AS") => t.get(t.nonws(n.unwrap() + 1)).and_then(word).map(ident_value),
        Some(Token::Word(a))
            if !(a.quote_style.is_none()
                && RELATION_STOP.iter().any(|s| a.value.eq_ignore_ascii_case(s))) =>
        {
            Some(ident_value(a))
        }
        _ => None,
    };
    Some(Relation { name, alias })
}

/// Parse and strip ExQL clauses from a query.
pub fn parse_query_spec(sql: &str) -> Result<QuerySpec, ExqlError> {
    let t = Toks::new(sql)?;
    extract(&t, 0)
}

fn extract(t: &Toks, start: usize) -> Result<QuerySpec, ExqlError> {
    let mut spec = QuerySpec::default();
    let mut out: Vec<Token> = Vec::with_capacity(t.len());
    let mut depth: i64 = 0;
    let mut in_from = false;
    let mut i = start;
    while i < t.len() {
        let tok = &t.t[i];
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ => {}
        }
        if depth != 0 {
            out.push(tok.clone());
            i += 1;
            continue;
        }
        if matches!(tok, Token::Comma) && in_from {
            if let Some(r) = peek_relation(t, i + 1) {
                spec.relations.push(r);
            }
        }
        let Some(w) = word(tok).filter(|w| w.quote_style.is_none()) else {
            out.push(tok.clone());
            i += 1;
            continue;
        };
        let upper = w.value.to_ascii_uppercase();
        match upper.as_str() {
            "FROM" => {
                in_from = true;
                if let Some(r) = peek_relation(t, i + 1) {
                    spec.relations.push(r);
                }
            }
            "JOIN" => {
                in_from = true;
                spec.within.push(None);
                if let Some(r) = peek_relation(t, i + 1) {
                    spec.relations.push(r);
                }
            }
            "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "UNION" => in_from = false,
            "EMIT" => {
                let n = t.nonws(i + 1);
                let emit = if t.is_kw(n, "CHANGES") {
                    Emit::Changes
                } else if t.is_kw(n, "FINAL") {
                    Emit::Final
                } else {
                    return Err(ExqlError::parse("expected EMIT CHANGES or EMIT FINAL"));
                };
                if t.nonws(n.unwrap() + 1).is_some() {
                    return Err(ExqlError::parse("EMIT must be the last clause"));
                }
                spec.emit = Some(emit);
                break;
            }
            "WINDOW" => {
                let n = t.nonws(i + 1);
                if t.is_kw(n, "TUMBLING") || t.is_kw(n, "HOPPING") || t.is_kw(n, "SESSION") {
                    if spec.window.is_some() {
                        return Err(ExqlError::parse("only one WINDOW clause is allowed"));
                    }
                    let hopping = t.is_kw(n, "HOPPING");
                    if t.is_kw(n, "SESSION") {
                        return Err(ExqlError::unsupported(
                            "SESSION windows",
                            "use WINDOW TUMBLING or WINDOW HOPPING",
                        ));
                    }
                    let (w, next) = parse_window(t, n.unwrap() + 1, hopping, &mut spec)?;
                    spec.window = Some(w);
                    i = next;
                    in_from = false;
                    continue;
                }
            }
            "GRACE" => {
                let mut n = t.nonws(i + 1);
                if t.is_kw(n, "PERIOD") {
                    n = t.nonws(n.unwrap() + 1);
                }
                let (ms, next) = parse_interval(t, n)?;
                set_grace(&mut spec, ms)?;
                i = next;
                continue;
            }
            "WITHIN" => {
                let Some(last) = spec.within.last_mut() else {
                    return Err(ExqlError::parse("WITHIN is only valid on a JOIN"));
                };
                if last.is_some() {
                    return Err(ExqlError::parse("WITHIN given twice for one JOIN"));
                }
                let (ms, next) = parse_interval(t, t.nonws(i + 1))?;
                if ms < 0 {
                    return Err(ExqlError::parse("WITHIN must not be negative"));
                }
                *last = Some(ms);
                i = next;
                continue;
            }
            "TIMESTAMP" if in_from && t.is_kw(t.nonws(i + 1), "BY") => {
                let from = t.nonws(i + 1).unwrap() + 1;
                let mut j = from;
                let mut d = 0i64;
                while j < t.len() {
                    let x = &t.t[j];
                    match x {
                        Token::LParen => d += 1,
                        Token::RParen => {
                            if d == 0 {
                                break;
                            }
                            d -= 1;
                        }
                        Token::Comma if d == 0 => break,
                        Token::Word(w) if d == 0 && w.quote_style.is_none() => {
                            let up = w.value.to_ascii_uppercase();
                            if TS_EXPR_STOP.contains(&up.as_str()) {
                                let is_fn = (up == "LEFT" || up == "RIGHT")
                                    && matches!(t.get(t.nonws(j + 1)), Some(Token::LParen));
                                if !is_fn {
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                let expr_sql = t.text(from, j);
                if expr_sql.is_empty() {
                    return Err(ExqlError::parse("TIMESTAMP BY needs an expression"));
                }
                let relation = spec
                    .relations
                    .last()
                    .map(|r| r.alias.clone().unwrap_or_else(|| r.name.clone()));
                if spec.timestamp_by.iter().any(|x| x.relation == relation) {
                    return Err(ExqlError::parse("TIMESTAMP BY given twice for one relation"));
                }
                spec.timestamp_by.push(TimestampBy { relation, expr_sql });
                i = j;
                continue;
            }
            _ => {}
        }
        out.push(tok.clone());
        i += 1;
    }
    spec.select_sql = render(&wrap_arrows(&out));
    Ok(spec)
}

fn set_grace(spec: &mut QuerySpec, ms: i64) -> Result<(), ExqlError> {
    if ms < 0 {
        return Err(ExqlError::parse("GRACE PERIOD must not be negative"));
    }
    match spec.grace_ms {
        Some(g) if g != ms => Err(ExqlError::parse("conflicting GRACE PERIOD values")),
        _ => {
            spec.grace_ms = Some(ms);
            Ok(())
        }
    }
}

/// `( SIZE <i> [, ADVANCE BY <i>] [, GRACE PERIOD <i>] )` starting at `i`.
fn parse_window(
    t: &Toks,
    i: usize,
    hopping: bool,
    spec: &mut QuerySpec,
) -> Result<(WindowSpec, usize), ExqlError> {
    let bad = |m: &str| ExqlError::parse(format!("invalid WINDOW clause: {m}"));
    let mut j = t.nonws(i).ok_or_else(|| bad("expected '('"))?;
    if !matches!(t.t[j], Token::LParen) {
        return Err(bad("expected '('"));
    }
    let mut size = None;
    let mut advance = None;
    loop {
        j = t.nonws(j + 1).ok_or_else(|| bad("unterminated"))?;
        if kw(&t.t[j], "SIZE") {
            let (ms, n) = parse_interval(t, t.nonws(j + 1))?;
            size = Some(ms);
            j = n;
        } else if kw(&t.t[j], "ADVANCE") {
            let mut n = t.nonws(j + 1);
            if t.is_kw(n, "BY") {
                n = t.nonws(n.unwrap() + 1);
            }
            let (ms, n) = parse_interval(t, n)?;
            advance = Some(ms);
            j = n;
        } else if kw(&t.t[j], "GRACE") {
            let mut n = t.nonws(j + 1);
            if t.is_kw(n, "PERIOD") {
                n = t.nonws(n.unwrap() + 1);
            }
            let (ms, n) = parse_interval(t, n)?;
            set_grace(spec, ms)?;
            j = n;
        } else {
            return Err(bad("expected SIZE, ADVANCE BY or GRACE PERIOD"));
        }
        let n = t.nonws(j).ok_or_else(|| bad("unterminated"))?;
        match t.t[n] {
            Token::Comma => j = n,
            Token::RParen => {
                j = n + 1;
                break;
            }
            _ => return Err(bad("expected ',' or ')'")),
        }
    }
    let size = size.ok_or_else(|| bad("SIZE is required"))?;
    if size <= 0 {
        return Err(bad("SIZE must be positive"));
    }
    let w = if hopping {
        let advance = advance.ok_or_else(|| bad("HOPPING windows need ADVANCE BY"))?;
        if advance <= 0 || advance > size {
            return Err(bad("ADVANCE BY must be positive and at most SIZE"));
        }
        if size / advance > 1000 {
            return Err(bad("SIZE / ADVANCE BY must be at most 1000"));
        }
        WindowSpec::Hopping {
            size_ms: size,
            advance_ms: advance,
        }
    } else {
        if advance.is_some() {
            return Err(bad("TUMBLING windows take no ADVANCE BY"));
        }
        WindowSpec::Tumbling { size_ms: size }
    };
    Ok((w, j))
}

fn parse_name(t: &Toks, i: Option<usize>) -> Result<(String, usize), ExqlError> {
    let i = i.ok_or_else(|| ExqlError::parse("expected a name"))?;
    let w = word(&t.t[i]).ok_or_else(|| ExqlError::parse("expected a name"))?;
    let name = ident_value(w);
    Ok((name, i + 1))
}

/// Validate an output stream / table name.
pub fn validate_object_name(name: &str) -> Result<(), ExqlError> {
    StreamName::try_from(name).map_err(|e| ExqlError::Plan(e.to_string()))?;
    if name.starts_with("__") {
        return Err(ExqlError::Plan(format!(
            "name '{name}' is reserved (names starting with '__' are internal)"
        )));
    }
    Ok(())
}

/// Validate a query id (used in file paths and stream names).
pub fn validate_query_id(id: &str) -> Result<(), ExqlError> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ExqlError::NotFound(format!("query '{id}' not found")));
    }
    Ok(())
}

/// Parse one ExQL statement.
pub fn parse_statement(sql: &str) -> Result<Statement, ExqlError> {
    let t = Toks::new(sql)?;
    let first = t.nonws(0).ok_or_else(|| ExqlError::parse("empty statement"))?;
    let second = t.nonws(first + 1);
    if t.is_kw(Some(first), "CREATE") {
        let mut n = second;
        let mut or_replace = false;
        if t.is_kw(n, "OR") {
            let r = t.nonws(n.unwrap() + 1);
            if !t.is_kw(r, "REPLACE") {
                return Err(ExqlError::parse("expected OR REPLACE"));
            }
            or_replace = true;
            n = t.nonws(r.unwrap() + 1);
        }
        if t.is_kw(n, "INDEX") || t.is_kw(n, "UNIQUE") {
            return Err(index_unsupported());
        }
        let kind = if t.is_kw(n, "STREAM") || t.is_kw(n, "VIEW") {
            CreateKind::Stream
        } else if t.is_kw(n, "TABLE") {
            CreateKind::Table
        } else if t.is_kw(n, "MATERIALIZED") {
            let v = t.nonws(n.unwrap() + 1);
            if !t.is_kw(v, "VIEW") {
                return Err(ExqlError::parse("expected MATERIALIZED VIEW"));
            }
            n = v;
            CreateKind::Table
        } else {
            return Err(ExqlError::unsupported(
                "this CREATE statement",
                "ExQL supports CREATE STREAM … AS SELECT and CREATE TABLE … AS SELECT",
            ));
        };
        let mut m = t.nonws(n.unwrap() + 1);
        let mut if_not_exists = false;
        if t.is_kw(m, "IF") {
            let a = t.nonws(m.unwrap() + 1);
            let b = t.nonws(a.map_or(t.len(), |x| x + 1));
            if !(t.is_kw(a, "NOT") && t.is_kw(b, "EXISTS")) {
                return Err(ExqlError::parse("expected IF NOT EXISTS"));
            }
            if_not_exists = true;
            m = t.nonws(b.unwrap() + 1);
        }
        let (name, after) = parse_name(&t, m)?;
        let as_kw = t.nonws(after);
        if !t.is_kw(as_kw, "AS") {
            return Err(ExqlError::parse(format!(
                "expected AS SELECT … after the name '{name}'"
            )));
        }
        let query = extract(&t, as_kw.unwrap() + 1)?;
        if query.select_sql.is_empty() {
            return Err(ExqlError::parse("expected a SELECT after AS"));
        }
        return Ok(Statement::Create(CreateQuery {
            kind,
            name,
            or_replace,
            if_not_exists,
            query,
        }));
    }
    if t.is_kw(Some(first), "DROP") {
        if t.is_kw(second, "INDEX") {
            return Err(index_unsupported());
        }
        if t.is_kw(second, "QUERY") {
            let (id, _) = parse_name(&t, t.nonws(second.unwrap() + 1))?;
            return Ok(Statement::DropQuery(id));
        }
        let mut n = second;
        let kind = if t.is_kw(n, "STREAM") || t.is_kw(n, "VIEW") {
            CreateKind::Stream
        } else if t.is_kw(n, "TABLE") {
            CreateKind::Table
        } else if t.is_kw(n, "MATERIALIZED") {
            n = t.nonws(n.unwrap() + 1);
            CreateKind::Table
        } else {
            return Err(ExqlError::unsupported(
                "this DROP statement",
                "ExQL supports DROP STREAM, DROP TABLE and DROP QUERY",
            ));
        };
        let mut m = t.nonws(n.unwrap() + 1);
        let mut if_exists = false;
        if t.is_kw(m, "IF") {
            let e = t.nonws(m.unwrap() + 1);
            if !t.is_kw(e, "EXISTS") {
                return Err(ExqlError::parse("expected IF EXISTS"));
            }
            if_exists = true;
            m = t.nonws(e.unwrap() + 1);
        }
        let (name, _) = parse_name(&t, m)?;
        return Ok(Statement::Drop {
            kind,
            name,
            if_exists,
        });
    }
    if (t.is_kw(Some(first), "PAUSE") || t.is_kw(Some(first), "RESUME"))
        && t.is_kw(second, "QUERY")
    {
        let (id, _) = parse_name(&t, t.nonws(second.unwrap() + 1))?;
        return Ok(if t.is_kw(Some(first), "PAUSE") {
            Statement::PauseQuery(id)
        } else {
            Statement::ResumeQuery(id)
        });
    }
    if t.is_kw(Some(first), "TERMINATE") {
        let mut n = second;
        if t.is_kw(n, "QUERY") {
            n = t.nonws(n.unwrap() + 1);
        }
        let (id, _) = parse_name(&t, n)?;
        return Ok(Statement::DropQuery(id));
    }
    // A bounded query: ExQL clauses are not allowed here.
    let spec = extract(&t, 0)?;
    if spec.has_extensions() {
        return Err(ExqlError::unsupported(
            "EMIT / WINDOW / WITHIN / GRACE / TIMESTAMP BY in a bounded query",
            "these clauses are for continuous queries: CREATE STREAM <name> AS SELECT … or CREATE TABLE <name> AS SELECT …",
        ));
    }
    Ok(Statement::Query(spec.select_sql))
}

fn index_unsupported() -> ExqlError {
    ExqlError::unsupported(
        "CREATE INDEX / DROP INDEX",
        "secondary indexes were removed; bounded queries push offset and timestamp predicates down to storage instead",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration_ms("5 minutes").unwrap(), 300_000);
        assert_eq!(parse_duration_ms("1h30m").unwrap(), 5_400_000);
        assert_eq!(parse_duration_ms("500ms").unwrap(), 500);
        assert_eq!(parse_duration_ms("1 week").unwrap(), 604_800_000);
        assert_eq!(parse_duration_ms("1.5 hours").unwrap(), 5_400_000);
        assert!(parse_duration_ms("5 fortnights").is_err());
        assert!(parse_duration_ms("").is_err());
    }

    #[test]
    fn create_stream_with_clauses() {
        let s = parse_statement(
            "CREATE STREAM out AS SELECT o.key, p.payload FROM orders o TIMESTAMP BY o.payload->>'ts' \
             JOIN payments AS p WITHIN INTERVAL '10 minutes' ON o.key = p.key \
             WHERE o.subject = 'x' GRACE PERIOD 5 SECONDS EMIT CHANGES;",
        )
        .unwrap();
        let Statement::Create(c) = s else { panic!() };
        assert_eq!(c.kind, CreateKind::Stream);
        assert_eq!(c.name, "out");
        assert_eq!(c.query.emit, Some(Emit::Changes));
        assert_eq!(c.query.grace_ms, Some(5_000));
        assert_eq!(c.query.within, vec![Some(600_000)]);
        assert_eq!(
            c.query.timestamp_by,
            vec![TimestampBy {
                relation: Some("o".into()),
                expr_sql: "(o.payload->>'ts')".into()
            }]
        );
        assert!(!c.query.select_sql.contains("WITHIN"));
        assert!(!c.query.select_sql.contains("TIMESTAMP BY"));
        assert!(!c.query.select_sql.contains("EMIT"));
        assert!(c.query.select_sql.contains("ON o.key = p.key"), "{}", c.query.select_sql);
        assert_eq!(c.query.relations[1].alias.as_deref(), Some("p"));
    }

    #[test]
    fn within_after_on() {
        let Statement::Create(c) = parse_statement(
            "CREATE VIEW x AS SELECT * FROM a JOIN b ON a.k = b.k WITHIN '1 minute' LEFT JOIN c ON a.k = c.k",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(c.query.within, vec![Some(60_000), None]);
    }

    #[test]
    fn windows() {
        let Statement::Create(c) = parse_statement(
            "CREATE TABLE t AS SELECT key, COUNT(*) FROM s WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 5 MINUTES, GRACE PERIOD 1 MINUTE) GROUP BY key EMIT FINAL",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(c.kind, CreateKind::Table);
        assert_eq!(
            c.query.window,
            Some(WindowSpec::Hopping {
                size_ms: 600_000,
                advance_ms: 300_000
            })
        );
        assert_eq!(c.query.grace_ms, Some(60_000));
        assert_eq!(c.query.emit, Some(Emit::Final));
        assert!(c.query.select_sql.contains("GROUP BY key"));
    }

    #[test]
    fn window_assignment() {
        let t = WindowSpec::Tumbling { size_ms: 10 };
        assert_eq!(t.assign(25), vec![(20, 30)]);
        assert_eq!(t.assign(-1), vec![(-10, 0)]);
        let h = WindowSpec::Hopping {
            size_ms: 10,
            advance_ms: 5,
        };
        assert_eq!(h.assign(12), vec![(5, 15), (10, 20)]);
        assert_eq!(h.assign(10), vec![(5, 15), (10, 20)]);
    }

    #[test]
    fn index_is_unsupported() {
        let e = parse_statement("CREATE INDEX i ON s(payload->>'x')").unwrap_err();
        assert_eq!(e.code(), "UNSUPPORTED");
        assert!(parse_statement("DROP INDEX i").is_err());
    }

    #[test]
    fn bounded_rejects_extensions() {
        assert!(parse_statement("SELECT * FROM s EMIT CHANGES").is_err());
        assert!(matches!(
            parse_statement("SELECT 'EMIT CHANGES', \"window\" FROM s").unwrap(),
            Statement::Query(_)
        ));
    }

    #[test]
    fn unicode_and_strings_are_safe() {
        let Statement::Create(c) = parse_statement(
            "CREATE STREAM ü AS SELECT 'WITHIN ''x''' AS a FROM s WHERE payload->>'名' = 'ü'",
        )
        .unwrap() else {
            panic!()
        };
        assert!(c.query.within.is_empty());
        assert!(c.query.select_sql.contains("'WITHIN ''x'''"));
    }

    #[test]
    fn arrows_are_parenthesized() {
        assert_eq!(
            normalize_sql("SELECT a.payload->'x'->>'y' IS NOT NULL, f(p)->>'k' > 2 FROM s").unwrap(),
            "SELECT ((a.payload->'x')->>'y') IS NOT NULL, (f(p)->>'k') > 2 FROM s"
        );
        assert_eq!(
            normalize_sql("SELECT 'it''s', \"we\"\"ird\" FROM s").unwrap(),
            "SELECT 'it''s', \"we\"\"ird\" FROM s"
        );
    }

    #[test]
    fn misc_statements() {
        assert_eq!(
            parse_statement("PAUSE QUERY abc").unwrap(),
            Statement::PauseQuery("abc".into())
        );
        assert_eq!(
            parse_statement("drop table if exists t").unwrap(),
            Statement::Drop {
                kind: CreateKind::Table,
                name: "t".into(),
                if_exists: true
            }
        );
        assert!(parse_statement("SELECT 1; SELECT 2").is_err());
    }
}
