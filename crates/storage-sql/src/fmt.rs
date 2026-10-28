//! SQL text of expressions (stored defaults and checks, catalogs, `SHOW CREATE TABLE`).

use std::fmt::{self, Display, Formatter};

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::datum::type_sql;

fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// An identifier, double-quoted when it is not a plain lower-case word.
pub fn quote_ident(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !s.starts_with(|c: char| c.is_ascii_digit());
    if plain {
        s.to_owned()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

fn op_str(op: &BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::IntDiv => "DIV",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::NullSafeEq => "<=>",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Xor => "XOR",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "#",
        BinaryOp::ShiftLeft => "<<",
        BinaryOp::ShiftRight => ">>",
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::Regex { case_insensitive: false, negated: false } => "~",
        BinaryOp::Regex { case_insensitive: true, negated: false } => "~*",
        BinaryOp::Regex { case_insensitive: false, negated: true } => "!~",
        BinaryOp::Regex { case_insensitive: true, negated: true } => "!~*",
        BinaryOp::Contains => "@>",
        BinaryOp::ContainedBy => "<@",
        BinaryOp::Overlap => "&&",
    }
}

fn list(f: &mut Formatter<'_>, items: &[Expr]) -> fmt::Result {
    for (i, e) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{e}")?;
    }
    Ok(())
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Null => f.write_str("NULL"),
            Expr::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            Expr::Number(n) => f.write_str(n),
            Expr::Str(s) => f.write_str(&quote_str(s)),
            Expr::Blob(b) => write!(f, "'\\x{}'::bytea", crate::hash::hex(b)),
            Expr::Param(i) => write!(f, "${}", i + 1),
            Expr::Default => f.write_str("DEFAULT"),
            Expr::Column(parts) => f.write_str(&parts.iter().map(|p| quote_ident(p)).collect::<Vec<_>>().join(".")),
            Expr::Variable { name, system } => write!(f, "{}{name}", if *system { "@@" } else { "@" }),
            Expr::Unary { op, expr } => match op {
                UnaryOp::Neg => write!(f, "(-{expr})"),
                UnaryOp::Not => write!(f, "(NOT {expr})"),
                UnaryOp::BitNot => write!(f, "(~{expr})"),
            },
            Expr::Binary { op, left, right } => write!(f, "({left} {} {right})", op_str(op)),
            Expr::Function { name, args, star, distinct, .. } => {
                write!(f, "{name}(")?;
                if *star {
                    f.write_str("*")?;
                } else {
                    if *distinct {
                        f.write_str("DISTINCT ")?;
                    }
                    list(f, args)?;
                }
                f.write_str(")")
            }
            Expr::Case { operand, whens, else_ } => {
                f.write_str("CASE")?;
                if let Some(o) = operand {
                    write!(f, " {o}")?;
                }
                for (w, t) in whens {
                    write!(f, " WHEN {w} THEN {t}")?;
                }
                if let Some(e) = else_ {
                    write!(f, " ELSE {e}")?;
                }
                f.write_str(" END")
            }
            Expr::Cast { expr, ty } => write!(f, "CAST({expr} AS {})", type_sql(ty)),
            Expr::IsNull { expr, negated } => write!(f, "({expr} IS {}NULL)", if *negated { "NOT " } else { "" }),
            Expr::IsBool { expr, value, negated } => {
                write!(f, "({expr} IS {}{})", if *negated { "NOT " } else { "" }, if *value { "TRUE" } else { "FALSE" })
            }
            Expr::IsDistinctFrom { left, right, negated } => {
                write!(f, "({left} IS {}DISTINCT FROM {right})", if *negated { "NOT " } else { "" })
            }
            Expr::Between { expr, low, high, negated } => {
                write!(f, "({expr} {}BETWEEN {low} AND {high})", if *negated { "NOT " } else { "" })
            }
            Expr::InList { expr, list: items, negated } => {
                write!(f, "({expr} {}IN (", if *negated { "NOT " } else { "" })?;
                list(f, items)?;
                f.write_str("))")
            }
            Expr::InSubquery { expr, negated, .. } => write!(f, "({expr} {}IN (subquery))", if *negated { "NOT " } else { "" }),
            Expr::AnyAll { left, op, right, all } => {
                write!(f, "({left} {} {}({right}))", op_str(op), if *all { "ALL" } else { "ANY" })
            }
            Expr::Exists { negated, .. } => write!(f, "({}EXISTS (subquery))", if *negated { "NOT " } else { "" }),
            Expr::Subquery(_) => f.write_str("(subquery)"),
            Expr::Like { expr, pattern, negated, case_insensitive, .. } => write!(
                f,
                "({expr} {}{} {pattern})",
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" }
            ),
            Expr::Tuple(items) => {
                f.write_str("(")?;
                list(f, items)?;
                f.write_str(")")
            }
            Expr::Array(items) => {
                f.write_str("ARRAY[")?;
                list(f, items)?;
                f.write_str("]")
            }
            Expr::Index { expr, index } => write!(f, "{expr}[{index}]"),
            Expr::Interval(s) => write!(f, "INTERVAL {}", quote_str(s)),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({field} FROM {expr})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::lexer::Dialect;
    use crate::parser::parse_expr;

    #[test]
    fn rendered_sql_parses_back_to_the_same_tree() {
        for sql in [
            "balance >= 0 AND balance < 1000000.00",
            "now()",
            "'it''s'",
            "CASE WHEN a IS NULL THEN 0 ELSE a + 1 END",
            "CAST('1.5' AS numeric(10,2))",
            "a NOT IN (1, 2, 3)",
            "lower(\"Email\") LIKE '%@x.com'",
        ] {
            let e = parse_expr(sql, Dialect::Postgres).unwrap();
            let again = parse_expr(&e.to_string(), Dialect::Postgres).unwrap();
            assert_eq!(e, again, "{sql} -> {e}");
        }
    }
}
