use sqlparser::dialect::{Dialect, Precedence};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

#[derive(Debug)]
pub struct ExspeedDialect;

// Precedence values modelled after PostgreSQL so that JSON operators
// (->>, ->, #>, etc.) bind tighter than comparison operators.
const PG_OTHER_PREC: u8 = 70;
const BETWEEN_LIKE_PREC: u8 = 60;
const EQ_PREC: u8 = 50;
const IS_PREC: u8 = 40;
const NOT_PREC: u8 = 30;
const AND_PREC: u8 = 20;
const OR_PREC: u8 = 10;

impl Dialect for ExspeedDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// Override the default precedence so that JSON operators (->>, ->, #>>,
    /// etc.) bind tighter than comparison operators like `=`.
    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let token = parser.peek_token();
        match token.token {
            Token::Arrow
            | Token::LongArrow
            | Token::HashArrow
            | Token::HashLongArrow
            | Token::AtArrow
            | Token::ArrowAt
            | Token::HashMinus
            | Token::AtQuestion
            | Token::AtAt
            | Token::Question
            | Token::QuestionAnd
            | Token::QuestionPipe
            | Token::CustomBinaryOperator(_) => Some(Ok(PG_OTHER_PREC)),
            _ => None, // fall back to default
        }
    }

    /// Matching prec_value so that the returned numeric value from
    /// `get_next_precedence` compares correctly against the thresholds
    /// the parser uses for other operators.
    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => 200,
            Precedence::DoubleColon => 140,
            Precedence::AtTz => 110,
            Precedence::MulDivModOp => 90,
            Precedence::PlusMinus => 80,
            Precedence::Xor => 75,
            Precedence::Ampersand => PG_OTHER_PREC,
            Precedence::Caret => 100,
            Precedence::Pipe => PG_OTHER_PREC,
            Precedence::Colon => PG_OTHER_PREC,
            Precedence::Between => BETWEEN_LIKE_PREC,
            Precedence::Eq => EQ_PREC,
            Precedence::Like => BETWEEN_LIKE_PREC,
            Precedence::Is => IS_PREC,
            Precedence::PgOther => PG_OTHER_PREC,
            Precedence::UnaryNot => NOT_PREC,
            Precedence::And => AND_PREC,
            Precedence::Or => OR_PREC,
        }
    }
}
