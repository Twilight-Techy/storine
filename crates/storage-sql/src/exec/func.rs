//! Built-in functions (scalar, aggregate, table), JSON helpers and result types.

use std::collections::BTreeMap;

use storage_types::{Decimal, RoundingMode};

use crate::ast::{DataType, Expr};
use crate::datum::{self, cast, compare, sql_cmp, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::{Env, Exec};
use crate::exec::select::{ColMeta, Rel};
use crate::lexer::Dialect;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_DAY: i64 = 86_400 * NS_PER_SEC;

fn arity(name: &str, args: &[Datum], min: usize, max: usize) -> SqlResult<()> {
    if args.len() < min || args.len() > max {
        return Err(SqlError::new(
            "42883",
            format!("function {name} does not accept {} arguments", args.len()),
        ));
    }
    Ok(())
}

fn text(d: &Datum) -> String {
    d.as_text()
}

fn int(d: &Datum) -> SqlResult<i64> {
    d.as_i64()
}

fn null_if_any(args: &[Datum]) -> bool {
    args.iter().any(Datum::is_null)
}

/// Calls a scalar function. Arguments are evaluated here so that lazy functions (COALESCE, IF)
/// evaluate only what they need.
pub fn call(
    ex: &Exec<'_>,
    name: &str,
    arg_exprs: &[Expr],
    star: bool,
    _distinct: bool,
    env: &Env<'_>,
) -> SqlResult<Datum> {
    // Lazy / special forms first.
    match name {
        "coalesce" | "ifnull" | "nvl" | "isnull" if !(name == "isnull" && arg_exprs.len() == 1) => {
            for a in arg_exprs {
                let v = ex.eval(a, env)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            return Ok(Datum::Null);
        }
        "if" | "iif" => {
            if arg_exprs.len() != 3 {
                return Err(SqlError::new("42883", "IF needs 3 arguments"));
            }
            return if ex.eval_bool(&arg_exprs[0], env)? {
                ex.eval(&arg_exprs[1], env)
            } else {
                ex.eval(&arg_exprs[2], env)
            };
        }
        "values" => {
            // MySQL ON DUPLICATE KEY UPDATE col = VALUES(col): the row proposed for insertion.
            if let [Expr::Column(p)] = arg_exprs {
                return ex.column(&["excluded".to_owned(), p[p.len() - 1].clone()], env);
            }
        }
        "array" => {
            if let [Expr::Subquery(q)] = arg_exprs {
                let rel = ex.subquery(q, env)?;
                return Ok(Datum::Array(
                    rel.rows
                        .iter()
                        .map(|r| r.first().cloned().unwrap_or(Datum::Null))
                        .collect(),
                ));
            }
        }
        "count" if star => {
            return Err(SqlError::new(
                "42803",
                "aggregate function count(*) is not allowed here",
            ))
        }
        _ => {}
    }
    let args = arg_exprs
        .iter()
        .map(|a| ex.eval(a, env))
        .collect::<SqlResult<Vec<_>>>()?;
    scalar(ex, name, &args)
}

pub fn scalar(ex: &Exec<'_>, name: &str, args: &[Datum]) -> SqlResult<Datum> {
    let mysql = ex.dialect() == Dialect::MySql;
    let a0 = args.first().cloned().unwrap_or(Datum::Null);
    Ok(match name {
        // ── strings ──
        "lower" | "lcase" => map_text(&a0, |s| s.to_lowercase()),
        "upper" | "ucase" => map_text(&a0, |s| s.to_uppercase()),
        "length" | "char_length" | "character_length" | "len" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Bytes(b) => Datum::Int(b.len() as i64),
            // MySQL LENGTH() counts bytes.
            d if mysql && name == "length" => Datum::Int(text(d).len() as i64),
            d => Datum::Int(text(d).chars().count() as i64),
        },
        "octet_length" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Bytes(b) => Datum::Int(b.len() as i64),
            d => Datum::Int(text(d).len() as i64),
        },
        "concat" => {
            // PostgreSQL CONCAT skips NULLs; MySQL's returns NULL if any argument is NULL.
            if mysql && null_if_any(args) {
                Datum::Null
            } else {
                Datum::Text(args.iter().filter(|d| !d.is_null()).map(text).collect())
            }
        }
        "concat_ws" => {
            if a0.is_null() {
                return Ok(Datum::Null);
            }
            let sep = text(&a0);
            Datum::Text(
                args[1..]
                    .iter()
                    .filter(|d| !d.is_null())
                    .map(text)
                    .collect::<Vec<_>>()
                    .join(&sep),
            )
        }
        "substring" | "substr" | "mid" => {
            arity(name, args, 2, 3)?;
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let chars: Vec<char> = text(&a0).chars().collect();
            let start = int(&args[1])?;
            let len = if args.len() > 2 {
                Some(int(&args[2])?)
            } else {
                None
            };
            // 1-based; MySQL negative starts count from the end.
            let begin = if start < 0 && mysql {
                chars.len() as i64 + start
            } else {
                start - 1
            };
            let end = match len {
                Some(l) => begin + l,
                None => chars.len() as i64,
            };
            let (b, e) = (
                begin.max(0) as usize,
                end.clamp(0, chars.len() as i64) as usize,
            );
            Datum::Text(if b < e {
                chars[b..e].iter().collect()
            } else {
                String::new()
            })
        }
        "left" | "right" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let chars: Vec<char> = text(&a0).chars().collect();
            let n = int(&args[1])?;
            let n = if n < 0 {
                (chars.len() as i64 + n).max(0) as usize
            } else {
                (n as usize).min(chars.len())
            };
            Datum::Text(if name == "left" {
                chars[..n].iter().collect()
            } else {
                chars[chars.len() - n..].iter().collect()
            })
        }
        "position" | "strpos" | "instr" | "locate" | "charindex" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            // position(needle, hay); strpos/instr(hay, needle); locate/charindex(needle, hay[, start]).
            let (needle, hay) = if matches!(name, "strpos" | "instr") {
                (text(&args[1]), text(&a0))
            } else {
                (text(&a0), text(&args[1]))
            };
            let start = if args.len() > 2 {
                int(&args[2])?.max(1) as usize - 1
            } else {
                0
            };
            let hay_chars: Vec<char> = hay.chars().collect();
            let tail: String = hay_chars.iter().skip(start).collect();
            Datum::Int(match tail.find(&needle) {
                Some(byte) => (tail[..byte].chars().count() + start + 1) as i64,
                None => 0,
            })
        }
        "replace" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let from = text(&args[1]);
            if from.is_empty() {
                a0.clone()
            } else {
                Datum::Text(text(&a0).replace(&from, &text(&args[2])))
            }
        }
        "trim" | "btrim" | "ltrim" | "rtrim" => {
            if a0.is_null() {
                return Ok(Datum::Null);
            }
            let s = text(&a0);
            let set: Vec<char> = if args.len() > 1 {
                text(&args[1]).chars().collect()
            } else {
                vec![' ']
            };
            let f = |c: char| set.contains(&c);
            Datum::Text(match name {
                "ltrim" => s.trim_start_matches(f).to_owned(),
                "rtrim" => s.trim_end_matches(f).to_owned(),
                _ => s.trim_matches(f).to_owned(),
            })
        }
        "lpad" | "rpad" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let s: Vec<char> = text(&a0).chars().collect();
            let n = int(&args[1])?.max(0) as usize;
            let fill: Vec<char> = if args.len() > 2 {
                text(&args[2]).chars().collect()
            } else {
                vec![' ']
            };
            if s.len() >= n || fill.is_empty() {
                return Ok(Datum::Text(s[..n.min(s.len())].iter().collect()));
            }
            let pad: String = fill.iter().cycle().take(n - s.len()).collect();
            let s: String = s.into_iter().collect();
            Datum::Text(if name == "lpad" { pad + &s } else { s + &pad })
        }
        "repeat" | "replicate" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let n = int(&args[1])?.clamp(0, 1_000_000) as usize;
            Datum::Text(text(&a0).repeat(n))
        }
        "reverse" => map_text(&a0, |s| s.chars().rev().collect()),
        "initcap" => map_text(&a0, |s| {
            let mut out = String::new();
            let mut up = true;
            for c in s.chars() {
                if c.is_alphanumeric() {
                    if up {
                        out.extend(c.to_uppercase());
                    } else {
                        out.extend(c.to_lowercase());
                    }
                    up = false;
                } else {
                    out.push(c);
                    up = true;
                }
            }
            out
        }),
        "split_part" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let n = int(&args[2])?;
            let s = text(&a0);
            let parts: Vec<&str> = s.split(text(&args[1]).as_str()).collect();
            let idx = if n > 0 { n - 1 } else { parts.len() as i64 + n };
            Datum::Text(
                usize::try_from(idx)
                    .ok()
                    .and_then(|i| parts.get(i))
                    .map_or(String::new(), |p| (*p).to_owned()),
            )
        }
        "starts_with" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            Datum::Bool(text(&a0).starts_with(&text(&args[1])))
        }
        "ascii" => match &a0 {
            Datum::Null => Datum::Null,
            d => Datum::Int(text(d).chars().next().map_or(0, |c| c as i64)),
        },
        "chr" | "char" => match &a0 {
            Datum::Null => Datum::Null,
            d => Datum::Text(
                char::from_u32(int(d)? as u32)
                    .map(String::from)
                    .unwrap_or_default(),
            ),
        },
        "md5" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Bytes(b) => Datum::Text(crate::hash::hex(&crate::hash::md5(b))),
            d => Datum::Text(crate::hash::hex(&crate::hash::md5(text(d).as_bytes()))),
        },
        "sha256" | "sha2" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Bytes(b) => Datum::Bytes(crate::hash::sha256(b).to_vec()),
            d if mysql => Datum::Text(crate::hash::hex(&crate::hash::sha256(text(d).as_bytes()))),
            d => Datum::Bytes(crate::hash::sha256(text(d).as_bytes()).to_vec()),
        },
        "sha1" | "sha" => match &a0 {
            Datum::Null => Datum::Null,
            d => Datum::Text(crate::hash::hex(&crate::hash::sha1(&d.to_wire_bytes(true)))),
        },
        "to_hex" | "hex" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Int(i) => Datum::Text(if mysql {
                format!("{i:X}")
            } else {
                format!("{i:x}")
            }),
            Datum::Bytes(b) => Datum::Text(crate::hash::hex(b).to_uppercase()),
            d => Datum::Text(crate::hash::hex(text(d).as_bytes()).to_uppercase()),
        },
        "encode" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let bytes = match &a0 {
                Datum::Bytes(b) => b.clone(),
                d => text(d).into_bytes(),
            };
            match text(&args[1]).to_ascii_lowercase().as_str() {
                "hex" => Datum::Text(crate::hash::hex(&bytes)),
                "base64" => Datum::Text(crate::hash::base64_encode(&bytes)),
                "escape" => Datum::Text(String::from_utf8_lossy(&bytes).into_owned()),
                f => {
                    return Err(SqlError::new(
                        "22023",
                        format!("unrecognized encoding: \"{f}\""),
                    ))
                }
            }
        }
        "decode" if !mysql => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let s = text(&a0);
            match text(&args[1]).to_ascii_lowercase().as_str() {
                "hex" => Datum::Bytes(datum::parse_bytea(&format!("\\x{s}"))),
                "base64" => Datum::Bytes(
                    crate::hash::base64_decode(s.trim())
                        .ok_or_else(|| SqlError::invalid_text("invalid base64"))?,
                ),
                "escape" => Datum::Bytes(s.into_bytes()),
                f => {
                    return Err(SqlError::new(
                        "22023",
                        format!("unrecognized encoding: \"{f}\""),
                    ))
                }
            }
        }
        "quote_ident" => map_text(&a0, |s| crate::fmt::quote_ident(&s)),
        "quote_literal" | "quote" => map_text(&a0, |s| format!("'{}'", s.replace('\'', "''"))),
        "quote_nullable" => match &a0 {
            Datum::Null => Datum::Text("NULL".into()),
            d => Datum::Text(format!("'{}'", text(d).replace('\'', "''"))),
        },
        "regexp_replace" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let flags = if args.len() > 3 {
                text(&args[3])
            } else {
                String::new()
            };
            let re = crate::exec::regex::Regex::new(&text(&args[1]), flags.contains('i'))?;
            Datum::Text(re.replace(&text(&a0), &text(&args[2]), flags.contains('g') || mysql))
        }
        "regexp_like" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let re = crate::exec::regex::Regex::new(&text(&args[1]), true)?;
            Datum::Bool(re.is_match(&text(&a0)))
        }
        "string_to_array" => {
            if null_if_any(&args[..2.min(args.len())]) {
                return Ok(Datum::Null);
            }
            let s = text(&a0);
            let sep = text(&args[1]);
            Datum::Array(if s.is_empty() {
                Vec::new()
            } else {
                s.split(sep.as_str())
                    .map(|p| Datum::Text(p.to_owned()))
                    .collect()
            })
        }
        "array_to_string" => match &a0 {
            Datum::Array(items) => {
                let sep = args.get(1).map(text).unwrap_or_default();
                Datum::Text(
                    items
                        .iter()
                        .filter(|d| !d.is_null())
                        .map(text)
                        .collect::<Vec<_>>()
                        .join(&sep),
                )
            }
            _ => Datum::Null,
        },
        "format" => {
            // format('%s and %L', a, b): %s plain, %I identifier, %L literal.
            let fmt = text(&a0);
            let mut out = String::new();
            let mut it = args[1..].iter();
            let mut chars = fmt.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '%' {
                    match chars.next() {
                        Some('s') => out.push_str(&it.next().map(text).unwrap_or_default()),
                        Some('I') => out.push_str(&crate::fmt::quote_ident(
                            &it.next().map(text).unwrap_or_default(),
                        )),
                        Some('L') => match it.next() {
                            Some(Datum::Null) | None => out.push_str("NULL"),
                            Some(d) => out.push_str(&format!("'{}'", text(d).replace('\'', "''"))),
                        },
                        Some('%') => out.push('%'),
                        Some(o) => {
                            out.push('%');
                            out.push(o);
                        }
                        None => out.push('%'),
                    }
                } else {
                    out.push(c);
                }
            }
            Datum::Text(out)
        }
        // ── numbers ──
        "abs" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Int(i) => Datum::Int(
                i.checked_abs()
                    .ok_or_else(|| SqlError::numeric_out_of_range("bigint out of range"))?,
            ),
            Datum::Float(f) => Datum::Float(f.abs()),
            d => Datum::Numeric(
                numeric(d)?
                    .checked_abs()
                    .map_err(|_| SqlError::numeric_out_of_range("out of range"))?,
            ),
        },
        "ceil" | "ceiling" => round_like(&a0, RoundingMode::Ceiling)?,
        "floor" => round_like(&a0, RoundingMode::Floor)?,
        "round" => match (&a0, args.get(1)) {
            (Datum::Null, _) | (_, Some(Datum::Null)) => Datum::Null,
            (Datum::Float(f), None) => Datum::Float(f.round()),
            (Datum::Float(f), Some(n)) => {
                let p = 10f64.powi(int(n)? as i32);
                Datum::Float((f * p).round() / p)
            }
            (Datum::Int(i), None) => Datum::Int(*i),
            (d, n) => {
                let places = n.map(int).transpose()?.unwrap_or(0);
                let x = numeric(d)?;
                if places >= 0 {
                    Datum::Numeric(
                        x.rescale(places.min(38) as u8, RoundingMode::HalfUp)
                            .map_err(|_| SqlError::numeric_out_of_range("out of range"))?,
                    )
                } else {
                    let p = Decimal::from_i128(10i128.pow((-places).min(30) as u32));
                    let q = x
                        .checked_div(p, 0, RoundingMode::HalfUp)
                        .and_then(|q| q.checked_mul(p))
                        .map_err(|_| SqlError::numeric_out_of_range("out of range"))?;
                    Datum::Numeric(q)
                }
            }
        },
        "trunc" | "truncate" => match (&a0, args.get(1)) {
            (Datum::Null, _) => Datum::Null,
            (Datum::Float(f), None) => Datum::Float(f.trunc()),
            (Datum::Float(f), Some(n)) => {
                let p = 10f64.powi(int(n)? as i32);
                Datum::Float((f * p).trunc() / p)
            }
            (Datum::Int(i), _) => Datum::Int(*i),
            (Datum::Timestamp(_) | Datum::Date(_), _) => {
                return scalar(ex, "date_trunc", &[Datum::Text("day".into()), a0.clone()])
            }
            (d, n) => {
                let places = n.map(int).transpose()?.unwrap_or(0).clamp(0, 38) as u8;
                Datum::Numeric(
                    numeric(d)?
                        .rescale(places, RoundingMode::Down)
                        .map_err(|_| SqlError::numeric_out_of_range("out of range"))?,
                )
            }
        },
        "mod" => {
            arity(name, args, 2, 2)?;
            datum::arith('%', &a0, &args[1])?
        }
        "div" => {
            arity(name, args, 2, 2)?;
            match datum::arith('/', &a0, &args[1])? {
                Datum::Numeric(d) => Datum::Numeric(d.rescale(0, RoundingMode::Down).unwrap_or(d)),
                Datum::Float(f) => Datum::Float(f.trunc()),
                other => other,
            }
        }
        "power" | "pow" => float2(args, f64::powf)?,
        "sqrt" => float1(&a0, f64::sqrt)?,
        "cbrt" => float1(&a0, f64::cbrt)?,
        "exp" => float1(&a0, f64::exp)?,
        "ln" => float1(&a0, f64::ln)?,
        "log10" => float1(&a0, f64::log10)?,
        "log" => {
            if args.len() == 2 {
                if null_if_any(args) {
                    return Ok(Datum::Null);
                }
                let (b, x) = (
                    datum::to_f64(&a0).unwrap_or(0.0),
                    datum::to_f64(&args[1]).unwrap_or(0.0),
                );
                Datum::Float(x.ln() / b.ln())
            } else if mysql {
                float1(&a0, f64::ln)?
            } else {
                float1(&a0, f64::log10)?
            }
        }
        "sign" => match &a0 {
            Datum::Null => Datum::Null,
            d => Datum::Int(match sql_cmp(d, &Datum::Int(0)) {
                Some(std::cmp::Ordering::Less) => -1,
                Some(std::cmp::Ordering::Greater) => 1,
                _ => 0,
            }),
        },
        "pi" => Datum::Float(std::f64::consts::PI),
        "random" | "rand" => {
            let b = crate::hash::random_bytes(8);
            let n = u64::from_le_bytes(b.try_into().unwrap_or([0; 8]));
            Datum::Float((n >> 11) as f64 / (1u64 << 53) as f64)
        }
        "greatest" | "least" => {
            let vals: Vec<&Datum> = args.iter().filter(|d| !d.is_null()).collect();
            if vals.is_empty() || (mysql && vals.len() < args.len()) {
                return Ok(Datum::Null);
            }
            let mut best = vals[0];
            for v in &vals[1..] {
                let o = compare(v, best);
                if (name == "greatest" && o == std::cmp::Ordering::Greater)
                    || (name == "least" && o == std::cmp::Ordering::Less)
                {
                    best = v;
                }
            }
            best.clone()
        }
        "max" | "min" if args.len() > 1 => {
            scalar(ex, if name == "max" { "greatest" } else { "least" }, args)?
        }
        "nullif" => {
            arity(name, args, 2, 2)?;
            if ex.equals(&a0, &args[1]) == Some(true) {
                Datum::Null
            } else {
                a0.clone()
            }
        }
        "isnull" => Datum::Bool(a0.is_null()),
        // ── dates ──
        "now"
        | "current_timestamp"
        | "localtimestamp"
        | "transaction_timestamp"
        | "statement_timestamp"
        | "getdate"
        | "sysdatetime"
        | "getutcdate"
        | "utc_timestamp"
        | "current_datetime" => Datum::Timestamp(round_micros(ex.now)),
        "clock_timestamp" | "sysdate" => Datum::Timestamp(round_micros(crate::exec::now_ns())),
        "current_date" | "curdate" | "today" | "utc_date" => {
            Datum::Date(ex.now.div_euclid(NS_PER_DAY) as i32)
        }
        "current_time" | "curtime" | "localtime" | "utc_time" => {
            Datum::Time(round_micros(ex.now.rem_euclid(NS_PER_DAY)))
        }
        "date_trunc" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            date_trunc(
                &text(&a0).to_ascii_lowercase(),
                &cast(&args[1], &DataType::Timestamp)?,
            )?
        }
        "date_part" | "datepart" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            extract(&text(&a0).to_ascii_lowercase(), &args[1])?
        }
        "year" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "quarter"
        | "week" | "dayofweek" | "dayofyear" => {
            if a0.is_null() {
                return Ok(Datum::Null);
            }
            let field = match name {
                "dayofmonth" => "day",
                "dayofweek" => "mysql_dow",
                "dayofyear" => "doy",
                f => f,
            };
            match extract(field, &a0)? {
                Datum::Numeric(d) => Datum::Int(Datum::Numeric(d).as_i64()?),
                other => other,
            }
        }
        "date" => match &a0 {
            Datum::Null => Datum::Null,
            d => cast(d, &DataType::Date)?,
        },
        "time" => match &a0 {
            Datum::Null => Datum::Null,
            d => cast(d, &DataType::Time)?,
        },
        "timestamp" | "datetime" => cast(&a0, &DataType::Timestamp)?,
        "to_timestamp" => match (&a0, args.get(1)) {
            (Datum::Null, _) => Datum::Null,
            (d, None) => {
                let secs =
                    datum::to_f64(d).ok_or_else(|| SqlError::invalid_text("invalid epoch"))?;
                Datum::Timestamp((secs * 1e9) as i64)
            }
            (d, Some(_fmt)) => cast(d, &DataType::Timestamp)?,
        },
        "to_date" | "str_to_date" => cast(&a0, &DataType::Date)?,
        "unix_timestamp" | "epoch" => match &a0 {
            Datum::Null if args.is_empty() => Datum::Int(ex.now / NS_PER_SEC),
            Datum::Null => Datum::Null,
            d => Datum::Int(match cast(d, &DataType::Timestamp)? {
                Datum::Timestamp(ns) => ns.div_euclid(NS_PER_SEC),
                _ => 0,
            }),
        },
        "from_unixtime" => match &a0 {
            Datum::Null => Datum::Null,
            d => Datum::Timestamp((datum::to_f64(d).unwrap_or(0.0) * 1e9) as i64),
        },
        "age" => {
            let a = cast(&a0, &DataType::Timestamp)?;
            let b = match args.get(1) {
                Some(x) => cast(x, &DataType::Timestamp)?,
                None => Datum::Timestamp(ex.now - ex.now.rem_euclid(NS_PER_DAY)),
            };
            datum::arith('-', &a, &b)?
        }
        "date_add" | "adddate" | "date_sub" | "subdate" | "timestampadd" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let iv = match &args[1] {
                Datum::Interval { .. } => args[1].clone(),
                d => datum::Datum::Interval {
                    months: 0,
                    days: int(d)?,
                    ns: 0,
                },
            };
            let op = if name.contains("sub") { '-' } else { '+' };
            datum::arith(op, &cast(&a0, &DataType::Timestamp)?, &iv)?
        }
        "datediff" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            let (a, b) = (
                cast(&a0, &DataType::Date)?,
                cast(&args[1], &DataType::Date)?,
            );
            match (a, b) {
                (Datum::Date(x), Datum::Date(y)) => Datum::Int(i64::from(x) - i64::from(y)),
                _ => Datum::Null,
            }
        }
        "to_char" | "date_format" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            Datum::Text(format_datetime(
                &a0,
                &text(&args[1]),
                name == "date_format",
            )?)
        }
        "make_date" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            Datum::Date(datum::days_from_civil(
                int(&a0)? as i32,
                int(&args[1])? as u32,
                int(&args[2])? as u32,
            ))
        }
        "timezone" => args.get(1).cloned().unwrap_or(Datum::Null),
        // MySQL CONVERT_TZ: fixed offsets and UTC; named zones are NULL, as in a MySQL
        // without time zone tables.
        "convert_tz" => {
            if null_if_any(args) || args.len() < 3 {
                return Ok(Datum::Null);
            }
            match (
                tz_offset_secs(&text(&args[1])),
                tz_offset_secs(&text(&args[2])),
            ) {
                (Some(from), Some(to)) => {
                    let Datum::Timestamp(ns) = cast(&a0, &DataType::Timestamp)? else {
                        return Ok(Datum::Null);
                    };
                    Datum::Timestamp(ns + (to - from) * 1_000_000_000)
                }
                _ => Datum::Null,
            }
        }
        // ── identity / session ──
        "version" => Datum::Text(if mysql {
            crate::MYSQL_VERSION.into()
        } else {
            crate::PG_VERSION_STRING.into()
        }),
        "current_database" | "database" | "schema" | "db_name" => {
            if mysql || name == "current_database" || name == "db_name" {
                Datum::Text(ex.s.database.clone())
            } else {
                Datum::Text("public".into())
            }
        }
        "current_schema" => Datum::Text(if mysql {
            ex.s.database.clone()
        } else {
            "public".into()
        }),
        "current_schemas" => Datum::Array(vec![
            Datum::Text("pg_catalog".into()),
            Datum::Text("public".into()),
        ]),
        "current_user" | "session_user" | "user" | "system_user" | "current_role"
        | "suser_sname" => Datum::Text(if mysql && name != "current_role" {
            format!("{}@%", ex.s.user)
        } else {
            ex.s.user.clone()
        }),
        "current_catalog" => Datum::Text(ex.s.database.clone()),
        "pg_backend_pid" | "connection_id" => Datum::Int(ex.s.id as i64),
        "last_insert_id" | "lastval" | "scope_identity" => Datum::Int(ex.s.last_insert_id),
        "row_count" => Datum::Int(ex.s.last_row_count),
        "found_rows" => Datum::Int(ex.s.last_found_rows as i64),
        "gen_random_uuid" | "uuid_generate_v4" | "uuid" | "newid" | "uuid_v4" => {
            let mut b: [u8; 16] = crate::hash::random_bytes(16).try_into().unwrap_or([0; 16]);
            b[6] = (b[6] & 0x0f) | 0x40;
            b[8] = (b[8] & 0x3f) | 0x80;
            if mysql && name == "uuid" {
                Datum::Text(datum::format_uuid(&b))
            } else {
                Datum::Uuid(b)
            }
        }
        "pg_typeof" => Datum::Text(a0.type_name().into()),
        "nextval" => Datum::Int(crate::exec::dml::nextval(&ex.s.engine, &text(&a0))?),
        "currval" => Datum::Int(ex.s.last_insert_id),
        "setval" => {
            let v = int(&args[1])?;
            crate::exec::dml::setval(&ex.s.engine, &text(&a0), v)?;
            Datum::Int(v)
        }
        "set_config" => args.get(1).cloned().unwrap_or(Datum::Null),
        "current_setting" => {
            let k = text(&a0).to_ascii_lowercase();
            match ex.s.vars.get(&k) {
                Some(v) => Datum::Text(v.clone()),
                None if k == "server_version" => Datum::Text(crate::PG_VERSION.into()),
                None if k == "server_version_num" => Datum::Text(crate::PG_VERSION_NUM.into()),
                None => Datum::Null,
            }
        }
        "pg_is_in_recovery" => Datum::Bool(false),
        "txid_current" | "pg_current_xact_id" => Datum::Int(ex.now / 1000),
        "pg_postmaster_start_time" => Datum::Timestamp(ex.now),
        "inet_server_addr" | "inet_client_addr" => Datum::Text("127.0.0.1".into()),
        "inet_server_port" => Datum::Int(5432),
        "pg_encoding_to_char" => Datum::Text("UTF8".into()),
        "pg_get_userbyid" => Datum::Text(ex.s.user.clone()),
        "has_table_privilege"
        | "has_schema_privilege"
        | "has_database_privilege"
        | "has_column_privilege"
        | "has_sequence_privilege"
        | "has_function_privilege"
        | "pg_has_role" => Datum::Bool(true),
        "obj_description" | "col_description" | "shobj_description" | "pg_get_viewdef"
        | "pg_get_triggerdef" => Datum::Null,
        n if n.starts_with("pg_") && n.ends_with("_is_visible") => Datum::Bool(true),
        "format_type" => crate::exec::system::format_type(&a0, args.get(1)),
        "pg_get_expr" => crate::exec::system::pg_get_expr(ex, &a0, args.get(1))?,
        "pg_get_constraintdef" => crate::exec::system::constraint_def(ex, &a0)?,
        "pg_get_indexdef" => {
            let column = match args.get(1) {
                Some(d) if !d.is_null() => d.as_i64()?,
                _ => 0,
            };
            crate::exec::system::index_def(ex, &a0, column)?
        }
        "pg_relation_size" | "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" => {
            Datum::Int(0)
        }
        "pg_size_pretty" => Datum::Text(format!("{} bytes", text(&a0))),
        "pg_get_serial_sequence" => {
            if null_if_any(args) {
                return Ok(Datum::Null);
            }
            Datum::Text(format!("{}_{}_seq", text(&a0), text(&args[1])))
        }
        "to_regclass" | "regclass" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Int(i) => Datum::Int(*i),
            d => {
                let tname = text(d);
                let bare = tname
                    .rsplit('.')
                    .next()
                    .unwrap_or(&tname)
                    .trim_matches('"')
                    .to_owned();
                match ex.s.engine.find_table(&bare)? {
                    Some(t) => Datum::Int(crate::exec::system::table_oid(&t.name)),
                    None if name == "to_regclass"
                        || crate::exec::system::is_system(None, &bare) =>
                    {
                        Datum::Null
                    }
                    None => return Err(SqlError::undefined_table(&bare)),
                }
            }
        },
        "regtype" | "to_regtype" => match &a0 {
            Datum::Null => Datum::Null,
            Datum::Int(i) => Datum::Int(*i),
            d => match crate::parser::parse_data_type(&text(d)) {
                Ok(t) => Datum::Int(i64::from(crate::exec::system::type_oid(&t))),
                Err(_) if name == "to_regtype" => Datum::Null,
                Err(_) => Datum::Int(25),
            },
        },
        "regnamespace" => match text(&a0).as_str() {
            "pg_catalog" => Datum::Int(crate::exec::system::NS_PG_CATALOG),
            "information_schema" => Datum::Int(crate::exec::system::NS_INFORMATION_SCHEMA),
            _ => Datum::Int(crate::exec::system::NS_PUBLIC),
        },
        "array_length" | "cardinality" | "array_upper" => match &a0 {
            Datum::Array(a) if !a.is_empty() || name == "cardinality" => Datum::Int(a.len() as i64),
            _ => Datum::Null,
        },
        "array_lower" => match &a0 {
            Datum::Array(a) if !a.is_empty() => Datum::Int(1),
            _ => Datum::Null,
        },
        "array_append" => match (&a0, args.get(1)) {
            (Datum::Array(a), Some(x)) => {
                let mut v = a.clone();
                v.push(x.clone());
                Datum::Array(v)
            }
            (Datum::Null, Some(x)) => Datum::Array(vec![x.clone()]),
            _ => Datum::Null,
        },
        "array_remove" => match (&a0, args.get(1)) {
            (Datum::Array(a), Some(x)) => Datum::Array(
                a.iter()
                    .filter(|v| ex.equals(v, x) != Some(true))
                    .cloned()
                    .collect(),
            ),
            _ => Datum::Null,
        },
        "array_position" => match (&a0, args.get(1)) {
            (Datum::Array(a), Some(x)) => a
                .iter()
                .position(|v| ex.equals(v, x) == Some(true))
                .map_or(Datum::Null, |p| Datum::Int(p as i64 + 1)),
            _ => Datum::Null,
        },
        // ── JSON ──
        "to_json" | "to_jsonb" | "row_to_json" => Datum::Json(datum::json_of(&a0)),
        "json_build_object" | "jsonb_build_object" | "json_object" => {
            let mut parts = Vec::new();
            for pair in args.chunks(2) {
                let k = pair.first().map(text).unwrap_or_default();
                let v = pair.get(1).map_or("null".to_owned(), datum::json_of);
                parts.push(format!("{}:{}", datum::json_string(&k), v));
            }
            Datum::Json(format!("{{{}}}", parts.join(",")))
        }
        "json_build_array" | "jsonb_build_array" | "json_array" => Datum::Json(format!(
            "[{}]",
            args.iter()
                .map(datum::json_of)
                .collect::<Vec<_>>()
                .join(",")
        )),
        "json_extract"
        | "json_extract_path"
        | "jsonb_extract_path"
        | "json_value"
        | "json_extract_path_text"
        | "jsonb_extract_path_text" => {
            if null_if_any(&args[..1.min(args.len())]) {
                return Ok(Datum::Null);
            }
            let as_text = name.ends_with("_text") || name == "json_value";
            let mut cur = text(&a0);
            if name.starts_with("json_extract") && !name.contains("path") || name == "json_value" {
                // MySQL path syntax: $.a.b[0]
                let path = args.get(1).map(text).unwrap_or_default();
                match json_path(&cur, &path)? {
                    Some(v) => {
                        let j = parse_json(&v)?;
                        return Ok(if as_text {
                            json_to_text(&j)
                        } else {
                            Datum::Json(v)
                        });
                    }
                    None => return Ok(Datum::Null),
                }
            }
            for k in &args[1..] {
                match json_get(&cur, k, false)? {
                    Datum::Json(s) => cur = s,
                    _ => return Ok(Datum::Null),
                }
            }
            if as_text {
                json_to_text(&parse_json(&cur)?)
            } else {
                Datum::Json(cur)
            }
        }
        "json_unquote" => match &a0 {
            Datum::Null => Datum::Null,
            d => match parse_json(&text(d)) {
                Ok(j) => json_to_text(&j),
                Err(_) => Datum::Text(text(d)),
            },
        },
        "json_typeof" | "jsonb_typeof" | "json_type" => match &a0 {
            Datum::Null => Datum::Null,
            d => Datum::Text(
                match parse_json(&text(d))? {
                    Json::Null => "null",
                    Json::Bool(_) => "boolean",
                    Json::Num(_) => "number",
                    Json::Str(_) => "string",
                    Json::Arr(_) => "array",
                    Json::Obj(_) => "object",
                }
                .into(),
            ),
        },
        "json_array_length" | "jsonb_array_length" | "json_length" => match &a0 {
            Datum::Null => Datum::Null,
            d => match parse_json(&text(d))? {
                Json::Arr(a) => Datum::Int(a.len() as i64),
                Json::Obj(o) if mysql => Datum::Int(o.len() as i64),
                _ => {
                    return Err(SqlError::new(
                        "22023",
                        "cannot get array length of a non-array",
                    ))
                }
            },
        },
        "json_valid" => Datum::Bool(parse_json(&text(&a0)).is_ok()),
        _ => {
            return Err(SqlError::new(
                "42883",
                format!("function {name}() does not exist (not supported by Storine SQL)"),
            ))
        }
    })
}

fn map_text(d: &Datum, f: impl FnOnce(String) -> String) -> Datum {
    match d {
        Datum::Null => Datum::Null,
        other => Datum::Text(f(other.as_text())),
    }
}

fn numeric(d: &Datum) -> SqlResult<Decimal> {
    datum::to_decimal(d)
        .ok_or_else(|| SqlError::datatype_mismatch(format!("{} is not a number", d.type_name())))
}

fn round_like(d: &Datum, mode: RoundingMode) -> SqlResult<Datum> {
    Ok(match d {
        Datum::Null => Datum::Null,
        Datum::Int(i) => Datum::Int(*i),
        Datum::Float(f) => Datum::Float(if mode == RoundingMode::Ceiling {
            f.ceil()
        } else {
            f.floor()
        }),
        other => Datum::Numeric(
            numeric(other)?
                .rescale(0, mode)
                .map_err(|_| SqlError::numeric_out_of_range("out of range"))?,
        ),
    })
}

fn float1(d: &Datum, f: fn(f64) -> f64) -> SqlResult<Datum> {
    Ok(match d {
        Datum::Null => Datum::Null,
        other => {
            Datum::Float(f(datum::to_f64(other)
                .ok_or_else(|| SqlError::datatype_mismatch("expected a number"))?))
        }
    })
}

fn float2(args: &[Datum], f: fn(f64, f64) -> f64) -> SqlResult<Datum> {
    if args.len() != 2 {
        return Err(SqlError::new("42883", "expected 2 arguments"));
    }
    if null_if_any(args) {
        return Ok(Datum::Null);
    }
    Ok(Datum::Float(f(
        datum::to_f64(&args[0]).unwrap_or(0.0),
        datum::to_f64(&args[1]).unwrap_or(0.0),
    )))
}

fn round_micros(ns: i64) -> i64 {
    ns - ns.rem_euclid(1000)
}

fn date_trunc(unit: &str, ts: &Datum) -> SqlResult<Datum> {
    let Datum::Timestamp(ns) = ts else {
        return Ok(Datum::Null);
    };
    let day = ns.div_euclid(NS_PER_DAY);
    let rem = ns.rem_euclid(NS_PER_DAY);
    let (y, m, _) = datum::civil_from_days(day as i32);
    let at = |d: i32| i64::from(d) * NS_PER_DAY;
    Ok(Datum::Timestamp(match unit {
        "microseconds" | "microsecond" => ns - ns.rem_euclid(1000),
        "milliseconds" | "millisecond" => ns - ns.rem_euclid(1_000_000),
        "second" => ns - ns.rem_euclid(NS_PER_SEC),
        "minute" => ns - ns.rem_euclid(60 * NS_PER_SEC),
        "hour" => ns - ns.rem_euclid(3600 * NS_PER_SEC),
        "day" => ns - rem,
        "week" => {
            // ISO weeks start on Monday; 1970-01-01 was a Thursday.
            let dow = (day + 3).rem_euclid(7);
            at((day - dow) as i32)
        }
        "month" => at(datum::days_from_civil(y, m, 1)),
        "quarter" => at(datum::days_from_civil(y, (m - 1) / 3 * 3 + 1, 1)),
        "year" => at(datum::days_from_civil(y, 1, 1)),
        "decade" => at(datum::days_from_civil(y - y.rem_euclid(10), 1, 1)),
        "century" => at(datum::days_from_civil(y - (y - 1).rem_euclid(100), 1, 1)),
        other => {
            return Err(SqlError::new(
                "22023",
                format!("unit \"{other}\" not recognized"),
            ))
        }
    }))
}

/// `EXTRACT(field FROM value)` (numeric result, like PostgreSQL 14+).
pub fn extract(field: &str, v: &Datum) -> SqlResult<Datum> {
    let ns = match v {
        Datum::Null => return Ok(Datum::Null),
        Datum::Timestamp(ns) => *ns,
        Datum::Date(d) => i64::from(*d) * NS_PER_DAY,
        Datum::Time(t) => *t,
        Datum::Interval { months, days, ns } => {
            return Ok(Datum::Int(match field {
                "year" | "years" => months / 12,
                "month" | "months" | "mon" => months % 12,
                "day" | "days" => *days,
                "hour" | "hours" => ns / (3600 * NS_PER_SEC),
                "minute" | "minutes" => (ns / (60 * NS_PER_SEC)) % 60,
                "second" | "seconds" => (ns / NS_PER_SEC) % 60,
                "epoch" => (months * 30 + days) * 86_400 + ns / NS_PER_SEC,
                _ => {
                    return Err(SqlError::new(
                        "22023",
                        format!("interval unit \"{field}\" not supported"),
                    ))
                }
            }))
        }
        other => match cast(other, &DataType::Timestamp)? {
            Datum::Timestamp(ns) => ns,
            _ => return Ok(Datum::Null),
        },
    };
    let day = ns.div_euclid(NS_PER_DAY);
    let rem = ns.rem_euclid(NS_PER_DAY);
    let (y, m, d) = datum::civil_from_days(day as i32);
    let n = |x: i64| Datum::Int(x);
    Ok(match field.trim_end_matches('s') {
        "year" => n(i64::from(y)),
        "month" | "mon" => n(i64::from(m)),
        "day" => n(i64::from(d)),
        "hour" => n(rem / (3600 * NS_PER_SEC)),
        "minute" => n((rem / (60 * NS_PER_SEC)) % 60),
        "second" => {
            let secs =
                Decimal::new(i128::from(rem % (60 * NS_PER_SEC)) / 1000, 6).unwrap_or_default();
            Datum::Numeric(secs.normalize())
        }
        "millisecond" => n((rem % (60 * NS_PER_SEC)) / 1_000_000),
        "microsecond" => n((rem % (60 * NS_PER_SEC)) / 1000),
        "quarter" => n(i64::from((m - 1) / 3 + 1)),
        "dow" => n((day + 4).rem_euclid(7)),
        "mysql_dow" => n((day + 4).rem_euclid(7) + 1),
        "isodow" => n((day + 3).rem_euclid(7) + 1),
        "doy" => n(day - i64::from(datum::days_from_civil(y, 1, 1)) + 1),
        "week" => {
            // ISO week number.
            let dow = (day + 3).rem_euclid(7); // Monday = 0
            let thursday = day - dow + 3;
            let (ty, _, _) = datum::civil_from_days(thursday as i32);
            n((thursday - i64::from(datum::days_from_civil(ty, 1, 1))) / 7 + 1)
        }
        "epoch" => {
            if ns % NS_PER_SEC == 0 {
                n(ns / NS_PER_SEC)
            } else {
                Datum::Numeric(
                    Decimal::new(i128::from(ns / 1000), 6)
                        .unwrap_or_default()
                        .normalize(),
                )
            }
        }
        "century" => n(i64::from((y - 1) / 100 + 1)),
        "decade" => n(i64::from(y / 10)),
        "julian" => n(day + 2_440_588),
        other => {
            return Err(SqlError::new(
                "22023",
                format!("unit \"{other}\" not recognized"),
            ))
        }
    })
}

/// A subset of PostgreSQL `to_char` patterns and MySQL `DATE_FORMAT` specifiers.
fn format_datetime(v: &Datum, fmt: &str, mysql_style: bool) -> SqlResult<String> {
    let ns = match cast(v, &DataType::Timestamp)? {
        Datum::Timestamp(ns) => ns,
        _ => return Ok(v.as_text()),
    };
    let day = ns.div_euclid(NS_PER_DAY);
    let rem = ns.rem_euclid(NS_PER_DAY);
    let (y, m, d) = datum::civil_from_days(day as i32);
    let (hh, mi, ss) = (
        rem / (3600 * NS_PER_SEC),
        (rem / (60 * NS_PER_SEC)) % 60,
        (rem / NS_PER_SEC) % 60,
    );
    let us = (rem % NS_PER_SEC) / 1000;
    let months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let days = [
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
    ];
    let wd = days[day.rem_euclid(7) as usize];
    let mut out = String::new();
    if mysql_style {
        let mut it = fmt.chars();
        while let Some(c) = it.next() {
            if c != '%' {
                out.push(c);
                continue;
            }
            match it.next() {
                Some('Y') => out.push_str(&format!("{y:04}")),
                Some('y') => out.push_str(&format!("{:02}", y % 100)),
                Some('m') => out.push_str(&format!("{m:02}")),
                Some('c') => out.push_str(&m.to_string()),
                Some('d') => out.push_str(&format!("{d:02}")),
                Some('e') => out.push_str(&d.to_string()),
                Some('H') => out.push_str(&format!("{hh:02}")),
                Some('h') | Some('I') => {
                    out.push_str(&format!("{:02}", if hh % 12 == 0 { 12 } else { hh % 12 }))
                }
                Some('i') => out.push_str(&format!("{mi:02}")),
                Some('s') | Some('S') => out.push_str(&format!("{ss:02}")),
                Some('f') => out.push_str(&format!("{us:06}")),
                Some('p') => out.push_str(if hh < 12 { "AM" } else { "PM" }),
                Some('M') => out.push_str(months[m as usize - 1]),
                Some('b') => out.push_str(&months[m as usize - 1][..3]),
                Some('W') => out.push_str(wd),
                Some('a') => out.push_str(&wd[..3]),
                Some('T') => out.push_str(&format!("{hh:02}:{mi:02}:{ss:02}")),
                Some('%') => out.push('%'),
                Some(o) => out.push(o),
                None => {}
            }
        }
        return Ok(out);
    }
    let mut i = 0;
    let f = fmt;
    while i < f.len() {
        let rest = &f[i..];
        let take = |p: &str| rest.starts_with(p);
        let (s, n) = if take("YYYY") {
            (format!("{y:04}"), 4)
        } else if take("YY") {
            (format!("{:02}", y % 100), 2)
        } else if take("MM") {
            (format!("{m:02}"), 2)
        } else if take("Month") {
            (months[m as usize - 1].to_owned(), 5)
        } else if take("Mon") {
            (months[m as usize - 1][..3].to_owned(), 3)
        } else if take("DD") {
            (format!("{d:02}"), 2)
        } else if take("Day") {
            (wd.to_owned(), 3)
        } else if take("Dy") {
            (wd[..3].to_owned(), 2)
        } else if take("HH24") {
            (format!("{hh:02}"), 4)
        } else if take("HH12") || take("HH") {
            (
                format!("{:02}", if hh % 12 == 0 { 12 } else { hh % 12 }),
                if take("HH12") { 4 } else { 2 },
            )
        } else if take("MI") {
            (format!("{mi:02}"), 2)
        } else if take("SS") {
            (format!("{ss:02}"), 2)
        } else if take("MS") {
            (format!("{:03}", us / 1000), 2)
        } else if take("US") {
            (format!("{us:06}"), 2)
        } else if take("AM") || take("PM") {
            ((if hh < 12 { "AM" } else { "PM" }).to_owned(), 2)
        } else {
            let c = rest.chars().next().unwrap_or(' ');
            (c.to_string(), c.len_utf8())
        };
        out.push_str(&s);
        i += n;
    }
    Ok(out)
}

// ── aggregates ──

pub fn finish_aggregate(
    ex: &Exec<'_>,
    name: &str,
    star: bool,
    count: i64,
    values: Vec<Datum>,
    separator: Option<String>,
) -> SqlResult<Datum> {
    Ok(match name {
        "count" => Datum::Int(if star { count } else { values.len() as i64 }),
        "sum" => {
            if values.is_empty() {
                return Ok(Datum::Null);
            }
            let mut acc = values[0].clone();
            for v in &values[1..] {
                acc = datum::arith('+', &acc, v)?;
            }
            // PostgreSQL sums integers as numeric (no overflow surprises); MySQL as DECIMAL.
            match acc {
                Datum::Int(i) => Datum::Numeric(Decimal::from_i64(i)),
                other => other,
            }
        }
        "avg" => {
            if values.is_empty() {
                return Ok(Datum::Null);
            }
            if values.iter().any(|v| matches!(v, Datum::Float(_))) {
                let s: f64 = values.iter().filter_map(datum::to_f64).sum();
                return Ok(Datum::Float(s / values.len() as f64));
            }
            let mut acc = Decimal::ZERO;
            let mut scale = 0u8;
            for v in &values {
                let d = numeric(v)?;
                scale = scale.max(d.scale());
                acc = acc
                    .checked_add(d)
                    .map_err(|_| SqlError::numeric_out_of_range("avg overflow"))?;
            }
            let n = Decimal::from_i64(values.len() as i64);
            let target = if ex.dialect() == Dialect::MySql {
                scale + 4
            } else {
                (scale + 16).min(20)
            };
            let q = acc
                .checked_div(n, target.min(38), RoundingMode::HalfUp)
                .map_err(|_| SqlError::numeric_out_of_range("avg overflow"))?;
            Datum::Numeric(if ex.dialect() == Dialect::MySql {
                q
            } else {
                trim_avg(q)
            })
        }
        "min" | "max" => {
            let mut it = values.into_iter();
            let Some(mut best) = it.next() else {
                return Ok(Datum::Null);
            };
            for v in it {
                let o = compare(&v, &best);
                if (name == "max" && o == std::cmp::Ordering::Greater)
                    || (name == "min" && o == std::cmp::Ordering::Less)
                {
                    best = v;
                }
            }
            best
        }
        "string_agg" | "group_concat" | "listagg" => {
            if values.is_empty() {
                return Ok(Datum::Null);
            }
            let sep = separator.unwrap_or_else(|| {
                if name == "group_concat" {
                    ",".into()
                } else {
                    String::new()
                }
            });
            Datum::Text(
                values
                    .iter()
                    .map(Datum::as_text)
                    .collect::<Vec<_>>()
                    .join(&sep),
            )
        }
        "array_agg" => {
            if values.is_empty() {
                Datum::Null
            } else {
                Datum::Array(values)
            }
        }
        "json_agg" | "jsonb_agg" => {
            if values.is_empty() {
                Datum::Null
            } else {
                Datum::Json(format!(
                    "[{}]",
                    values
                        .iter()
                        .map(datum::json_of)
                        .collect::<Vec<_>>()
                        .join(",")
                ))
            }
        }
        "bool_and" | "every" => {
            if values.is_empty() {
                return Ok(Datum::Null);
            }
            let mut r = true;
            for v in &values {
                r &= v.truthy()?.unwrap_or(true);
            }
            Datum::Bool(r)
        }
        "bool_or" => {
            if values.is_empty() {
                return Ok(Datum::Null);
            }
            let mut r = false;
            for v in &values {
                r |= v.truthy()?.unwrap_or(false);
            }
            Datum::Bool(r)
        }
        "bit_and" | "bit_or" => {
            if values.is_empty() {
                return Ok(Datum::Null);
            }
            let mut r = if name == "bit_and" { -1i64 } else { 0 };
            for v in &values {
                let x = v.as_i64()?;
                r = if name == "bit_and" { r & x } else { r | x };
            }
            Datum::Int(r)
        }
        "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop" => {
            let xs: Vec<f64> = values.iter().filter_map(datum::to_f64).collect();
            let pop = name.ends_with("_pop");
            let n = xs.len() as f64;
            if xs.is_empty() || (!pop && xs.len() < 2) {
                return Ok(Datum::Null);
            }
            let mean = xs.iter().sum::<f64>() / n;
            let var =
                xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / if pop { n } else { n - 1.0 };
            Datum::Float(if name.starts_with("stddev") {
                var.sqrt()
            } else {
                var
            })
        }
        other => return Err(SqlError::unsupported(format!("aggregate {other}()"))),
    })
}

/// PostgreSQL shows averages with the significant digits only (`2.5000000000000000` → `2.5`)
/// unless the inputs had a scale, which is kept.
fn trim_avg(d: Decimal) -> Decimal {
    d.normalize()
}

/// Seconds east of UTC for `UTC`, `Z`, `GMT`, `SYSTEM` (the server runs in UTC) or `±hh:mm`.
fn tz_offset_secs(z: &str) -> Option<i64> {
    let z = z.trim();
    if ["utc", "z", "gmt", "system", "etc/utc"]
        .iter()
        .any(|n| z.eq_ignore_ascii_case(n))
    {
        return Some(0);
    }
    let (sign, rest) = match z.as_bytes().first()? {
        b'+' => (1, &z[1..]),
        b'-' => (-1, &z[1..]),
        _ => return None,
    };
    let (h, m) = rest.split_once(':').unwrap_or((rest, "0"));
    let (h, m) = (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?);
    (h <= 14 && m < 60).then_some(sign * (h * 3600 + m * 60))
}

// ── table functions ──

/// Reserved words reported by `pg_get_keywords()` and `information_schema.keywords`.
pub const RESERVED_WORDS: &[&str] = &[
    "ALL",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASYMMETRIC",
    "BOTH",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONSTRAINT",
    "CREATE",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEFAULT",
    "DEFERRABLE",
    "DESC",
    "DISTINCT",
    "DO",
    "ELSE",
    "END",
    "EXCEPT",
    "FALSE",
    "FETCH",
    "FOR",
    "FOREIGN",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IN",
    "INITIALLY",
    "INTERSECT",
    "INTO",
    "LATERAL",
    "LEADING",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "PLACING",
    "PRIMARY",
    "REFERENCES",
    "RETURNING",
    "SELECT",
    "SESSION_USER",
    "SOME",
    "SYMMETRIC",
    "TABLE",
    "THEN",
    "TO",
    "TRAILING",
    "TRUE",
    "UNION",
    "UNIQUE",
    "USER",
    "USING",
    "VARIADIC",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

pub fn table_function(ex: &Exec<'_>, name: &str, args: &[Datum]) -> SqlResult<Rel> {
    match name {
        "generate_series" => {
            if args.len() < 2 || null_if_any(args) {
                return Err(SqlError::new(
                    "42883",
                    "generate_series needs a start and a stop",
                ));
            }
            let mut rows = Vec::new();
            match (&args[0], &args[1]) {
                (Datum::Timestamp(_) | Datum::Date(_) | Datum::Text(_), _)
                    if matches!(
                        args.get(2),
                        Some(Datum::Interval { .. }) | Some(Datum::Text(_))
                    ) =>
                {
                    let mut cur = cast(&args[0], &DataType::Timestamp)?;
                    let stop = cast(&args[1], &DataType::Timestamp)?;
                    let step = cast(&args[2], &DataType::Interval)?;
                    while compare(&cur, &stop) != std::cmp::Ordering::Greater
                        && rows.len() < 1_000_000
                    {
                        rows.push(vec![cur.clone()]);
                        cur = datum::arith('+', &cur, &step)?;
                    }
                    return Ok(Rel {
                        cols: vec![ColMeta::new("generate_series", Some(DataType::TimestampTz))],
                        rows,
                    });
                }
                _ => {
                    let (a, b) = (int(&args[0])?, int(&args[1])?);
                    let step = if args.len() > 2 { int(&args[2])? } else { 1 };
                    if step == 0 {
                        return Err(SqlError::new("22023", "step size cannot equal zero"));
                    }
                    let mut x = a;
                    while (step > 0 && x <= b) || (step < 0 && x >= b) {
                        rows.push(vec![Datum::Int(x)]);
                        if rows.len() > 10_000_000 {
                            return Err(SqlError::new(
                                "54000",
                                "generate_series is limited to 10,000,000 rows",
                            ));
                        }
                        x += step;
                    }
                }
            }
            Ok(Rel {
                cols: vec![ColMeta::new("generate_series", Some(DataType::BigInt))],
                rows,
            })
        }
        "generate_subscripts" => {
            // int2vector / oidvector text ("1 2") is zero-based; arrays are one-based.
            let (lo, n) = match args.first() {
                Some(Datum::Array(a)) => (1, a.len()),
                Some(Datum::Text(s)) if s.trim_start().starts_with('{') => {
                    (1, datum::parse_array(s)?.len())
                }
                Some(Datum::Text(s)) => (0, s.split_whitespace().count()),
                _ => (1, 0),
            };
            let rows = (0..n as i64).map(|k| vec![Datum::Int(lo + k)]).collect();
            Ok(Rel {
                cols: vec![ColMeta::new("generate_subscripts", Some(DataType::Int))],
                rows,
            })
        }
        "pg_get_keywords" => {
            let cols = ["word", "catcode", "barelabel", "catdesc", "baredesc"]
                .iter()
                .map(|c| {
                    ColMeta::new(
                        *c,
                        Some(if *c == "barelabel" {
                            DataType::Bool
                        } else {
                            DataType::Text
                        }),
                    )
                })
                .collect();
            let rows = RESERVED_WORDS
                .iter()
                .map(|w| {
                    vec![
                        Datum::Text(w.to_lowercase()),
                        Datum::Text("R".into()),
                        Datum::Bool(true),
                        Datum::Text("reserved".into()),
                        Datum::Text("can be bare label".into()),
                    ]
                })
                .collect();
            Ok(Rel { cols, rows })
        }
        "unnest" => {
            // Several arrays zip into columns, padded with NULLs to the longest.
            let arrays = args
                .iter()
                .map(|a| match a {
                    Datum::Array(a) => Ok(a.clone()),
                    Datum::Text(s) => datum::parse_array(s),
                    _ => Ok(Vec::new()),
                })
                .collect::<SqlResult<Vec<_>>>()?;
            let n = arrays.iter().map(Vec::len).max().unwrap_or(0);
            let rows = (0..n)
                .map(|i| {
                    arrays
                        .iter()
                        .map(|a| a.get(i).cloned().unwrap_or(Datum::Null))
                        .collect()
                })
                .collect();
            // Distinct names so `SELECT *` can expand them (PostgreSQL repeats "unnest").
            let cols = (0..arrays.len().max(1))
                .map(|i| {
                    ColMeta::new(
                        if i == 0 {
                            "unnest".to_owned()
                        } else {
                            format!("unnest_{}", i + 1)
                        },
                        None,
                    )
                })
                .collect();
            Ok(Rel { cols, rows })
        }
        "json_array_elements"
        | "jsonb_array_elements"
        | "json_array_elements_text"
        | "jsonb_array_elements_text" => {
            let j = parse_json(&args.first().map(text).unwrap_or_default())?;
            let Json::Arr(items) = j else {
                return Err(SqlError::new(
                    "22023",
                    "cannot extract elements from a non-array",
                ));
            };
            let as_text = name.ends_with("_text");
            Ok(Rel {
                cols: vec![ColMeta::new(
                    "value",
                    Some(if as_text {
                        DataType::Text
                    } else {
                        DataType::Json
                    }),
                )],
                rows: items
                    .into_iter()
                    .map(|x| {
                        vec![if as_text {
                            json_to_text(&x)
                        } else {
                            Datum::Json(json_stringify(&x))
                        }]
                    })
                    .collect(),
            })
        }
        "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text" => {
            let j = parse_json(&args.first().map(text).unwrap_or_default())?;
            let Json::Obj(m) = j else {
                return Err(SqlError::new(
                    "22023",
                    "cannot call json_each on a non-object",
                ));
            };
            let as_text = name.ends_with("_text");
            Ok(Rel {
                cols: vec![
                    ColMeta::new("key", Some(DataType::Text)),
                    ColMeta::new(
                        "value",
                        Some(if as_text {
                            DataType::Text
                        } else {
                            DataType::Json
                        }),
                    ),
                ],
                rows: m
                    .into_iter()
                    .map(|(k, v)| {
                        vec![
                            Datum::Text(k),
                            if as_text {
                                json_to_text(&v)
                            } else {
                                Datum::Json(json_stringify(&v))
                            },
                        ]
                    })
                    .collect(),
            })
        }
        // A scalar function in FROM is a one-row table named after it
        // (`SELECT * FROM current_schema()`).
        other => {
            let v = scalar(ex, other, args).map_err(|e| {
                if e.code == "42883" {
                    SqlError::new("42883", format!("function {other}() does not exist"))
                } else {
                    e
                }
            })?;
            Ok(Rel {
                cols: vec![ColMeta::new(other, None)],
                rows: vec![vec![v]],
            })
        }
    }
}

// ── JSON ──

#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(BTreeMap<String, Json>),
}

pub fn parse_json(s: &str) -> SqlResult<Json> {
    let b = s.as_bytes();
    let mut i = 0;
    let v = json_value(b, &mut i, 0)?;
    skip_ws(b, &mut i);
    if i != b.len() {
        return Err(SqlError::invalid_text("invalid input syntax for type json"));
    }
    Ok(v)
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && b[*i].is_ascii_whitespace() {
        *i += 1;
    }
}

fn json_value(b: &[u8], i: &mut usize, depth: usize) -> SqlResult<Json> {
    let bad = || SqlError::invalid_text("invalid input syntax for type json");
    if depth > 64 {
        return Err(bad());
    }
    skip_ws(b, i);
    match b.get(*i) {
        Some(b'{') => {
            *i += 1;
            let mut m = BTreeMap::new();
            skip_ws(b, i);
            if b.get(*i) == Some(&b'}') {
                *i += 1;
                return Ok(Json::Obj(m));
            }
            loop {
                skip_ws(b, i);
                let Json::Str(k) = json_value(b, i, depth + 1)? else {
                    return Err(bad());
                };
                skip_ws(b, i);
                if b.get(*i) != Some(&b':') {
                    return Err(bad());
                }
                *i += 1;
                let v = json_value(b, i, depth + 1)?;
                m.insert(k, v);
                skip_ws(b, i);
                match b.get(*i) {
                    Some(b',') => *i += 1,
                    Some(b'}') => {
                        *i += 1;
                        return Ok(Json::Obj(m));
                    }
                    _ => return Err(bad()),
                }
            }
        }
        Some(b'[') => {
            *i += 1;
            let mut a = Vec::new();
            skip_ws(b, i);
            if b.get(*i) == Some(&b']') {
                *i += 1;
                return Ok(Json::Arr(a));
            }
            loop {
                a.push(json_value(b, i, depth + 1)?);
                skip_ws(b, i);
                match b.get(*i) {
                    Some(b',') => *i += 1,
                    Some(b']') => {
                        *i += 1;
                        return Ok(Json::Arr(a));
                    }
                    _ => return Err(bad()),
                }
            }
        }
        Some(b'"') => {
            *i += 1;
            let mut out: Vec<u8> = Vec::new();
            while let Some(&c) = b.get(*i) {
                *i += 1;
                match c {
                    b'"' => return String::from_utf8(out).map(Json::Str).map_err(|_| bad()),
                    b'\\' => {
                        let e = *b.get(*i).ok_or_else(bad)?;
                        *i += 1;
                        match e {
                            b'n' => out.push(b'\n'),
                            b't' => out.push(b'\t'),
                            b'r' => out.push(b'\r'),
                            b'b' => out.push(8),
                            b'f' => out.push(12),
                            b'u' => {
                                let h = std::str::from_utf8(b.get(*i..*i + 4).ok_or_else(bad)?)
                                    .map_err(|_| bad())?;
                                let cp = u32::from_str_radix(h, 16).map_err(|_| bad())?;
                                *i += 4;
                                let ch = char::from_u32(cp).unwrap_or('\u{FFFD}');
                                let mut buf = [0u8; 4];
                                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                            }
                            other => out.push(other),
                        }
                    }
                    c => out.push(c),
                }
            }
            Err(bad())
        }
        Some(b't') if b[*i..].starts_with(b"true") => {
            *i += 4;
            Ok(Json::Bool(true))
        }
        Some(b'f') if b[*i..].starts_with(b"false") => {
            *i += 5;
            Ok(Json::Bool(false))
        }
        Some(b'n') if b[*i..].starts_with(b"null") => {
            *i += 4;
            Ok(Json::Null)
        }
        Some(c) if c.is_ascii_digit() || *c == b'-' => {
            let start = *i;
            while *i < b.len()
                && (b[*i].is_ascii_digit() || matches!(b[*i], b'-' | b'+' | b'.' | b'e' | b'E'))
            {
                *i += 1;
            }
            Ok(Json::Num(
                String::from_utf8_lossy(&b[start..*i]).into_owned(),
            ))
        }
        _ => Err(bad()),
    }
}

pub fn json_stringify(j: &Json) -> String {
    match j {
        Json::Null => "null".into(),
        Json::Bool(b) => b.to_string(),
        Json::Num(n) => n.clone(),
        Json::Str(s) => datum::json_string(s),
        Json::Arr(a) => format!(
            "[{}]",
            a.iter().map(json_stringify).collect::<Vec<_>>().join(",")
        ),
        Json::Obj(m) => format!(
            "{{{}}}",
            m.iter()
                .map(|(k, v)| format!("{}:{}", datum::json_string(k), json_stringify(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

fn json_to_text(j: &Json) -> Datum {
    match j {
        Json::Null => Datum::Null,
        Json::Str(s) => Datum::Text(s.clone()),
        other => Datum::Text(json_stringify(other)),
    }
}

/// `json -> key_or_index` (`as_text`: `->>`).
pub fn json_get(json: &str, key: &Datum, as_text: bool) -> SqlResult<Datum> {
    let j = parse_json(json)?;
    let v = match (&j, key) {
        (Json::Obj(m), Datum::Text(k)) => m.get(k),
        (Json::Arr(a), Datum::Int(n)) => {
            let idx = if *n < 0 { a.len() as i64 + n } else { *n };
            usize::try_from(idx).ok().and_then(|i| a.get(i))
        }
        (Json::Obj(m), other) => m.get(&other.as_text()),
        _ => None,
    };
    Ok(match v {
        None => Datum::Null,
        Some(x) if as_text => json_to_text(x),
        Some(x) => Datum::Json(json_stringify(x)),
    })
}

/// MySQL JSON path `$.a.b[0]` → the JSON text found, if any.
fn json_path(json: &str, path: &str) -> SqlResult<Option<String>> {
    let mut cur = parse_json(json)?;
    let p = path.trim();
    let p = p
        .strip_prefix('$')
        .ok_or_else(|| SqlError::new("22023", format!("invalid JSON path {path}")))?;
    let mut rest = p;
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            let end = r.find(['.', '[']).unwrap_or(r.len());
            let key = r[..end].trim_matches('"');
            cur = match cur {
                Json::Obj(mut m) => match m.remove(key) {
                    Some(v) => v,
                    None => return Ok(None),
                },
                _ => return Ok(None),
            };
            rest = &r[end..];
        } else if let Some(r) = rest.strip_prefix('[') {
            let end = r
                .find(']')
                .ok_or_else(|| SqlError::new("22023", format!("invalid JSON path {path}")))?;
            let idx: usize = r[..end]
                .trim()
                .parse()
                .map_err(|_| SqlError::new("22023", format!("invalid JSON path {path}")))?;
            cur = match cur {
                Json::Arr(mut a) if idx < a.len() => a.swap_remove(idx),
                _ => return Ok(None),
            };
            rest = &r[end + 1..];
        } else {
            return Err(SqlError::new("22023", format!("invalid JSON path {path}")));
        }
    }
    Ok(Some(json_stringify(&cur)))
}

pub fn json_concat(a: &str, b: &str) -> SqlResult<String> {
    Ok(match (parse_json(a)?, parse_json(b)?) {
        (Json::Obj(mut x), Json::Obj(y)) => {
            x.extend(y);
            json_stringify(&Json::Obj(x))
        }
        (Json::Arr(mut x), Json::Arr(y)) => {
            x.extend(y);
            json_stringify(&Json::Arr(x))
        }
        (Json::Arr(mut x), y) => {
            x.push(y);
            json_stringify(&Json::Arr(x))
        }
        (x, y) => json_stringify(&Json::Arr(vec![x, y])),
    })
}

/// `@>` on JSON: every key / element of `small` is in `big`.
pub fn json_contains(big: &str, small: &str) -> SqlResult<bool> {
    fn contains(b: &Json, s: &Json) -> bool {
        match (b, s) {
            (Json::Obj(bm), Json::Obj(sm)) => sm
                .iter()
                .all(|(k, v)| bm.get(k).is_some_and(|bv| contains(bv, v))),
            (Json::Arr(ba), Json::Arr(sa)) => {
                sa.iter().all(|v| ba.iter().any(|bv| contains(bv, v)))
            }
            (Json::Arr(ba), scalar) => ba.iter().any(|bv| bv == scalar),
            (Json::Num(x), Json::Num(y)) => x.parse::<f64>().ok() == y.parse::<f64>().ok(),
            (x, y) => x == y,
        }
    }
    Ok(contains(&parse_json(big)?, &parse_json(small)?))
}

/// The result type of a function call (for result metadata), when it is fixed.
pub fn return_type(
    ex: &Exec<'_>,
    name: &str,
    args: &[Expr],
    star: bool,
    cols: &[ColMeta],
) -> Option<DataType> {
    let mysql = ex.dialect() == Dialect::MySql;
    let arg_ty = |i: usize| args.get(i).and_then(|a| ex.static_type(a, cols).0);
    Some(match name {
        "count" => DataType::BigInt,
        "sum" => match arg_ty(0) {
            Some(DataType::Real | DataType::Double) => DataType::Double,
            _ => DataType::Numeric(None),
        },
        "avg" => match arg_ty(0) {
            Some(DataType::Real | DataType::Double) => DataType::Double,
            _ => DataType::Numeric(None),
        },
        "min" | "max" | "coalesce" | "ifnull" | "nvl" | "nullif" | "greatest" | "least" => {
            return arg_ty(0).or_else(|| arg_ty(1))
        }
        "lower"
        | "upper"
        | "concat"
        | "concat_ws"
        | "substring"
        | "substr"
        | "replace"
        | "trim"
        | "btrim"
        | "ltrim"
        | "rtrim"
        | "lpad"
        | "rpad"
        | "left"
        | "right"
        | "repeat"
        | "reverse"
        | "initcap"
        | "split_part"
        | "md5"
        | "to_char"
        | "date_format"
        | "string_agg"
        | "group_concat"
        | "version"
        | "current_database"
        | "database"
        | "current_schema"
        | "current_user"
        | "session_user"
        | "user"
        | "format_type"
        | "pg_get_expr"
        | "pg_get_constraintdef"
        | "pg_get_indexdef"
        | "quote_ident"
        | "quote_literal"
        | "format"
        | "regexp_replace"
        | "to_hex"
        | "hex"
        | "encode"
        | "json_unquote"
        | "pg_typeof"
        | "current_setting"
        | "array_to_string" => DataType::Text,
        "length" | "char_length" | "character_length" | "octet_length" | "position" | "strpos"
        | "instr" | "locate" | "ascii" | "pg_backend_pid" | "connection_id" | "last_insert_id"
        | "lastval" | "nextval" | "currval" | "setval" | "row_count" | "found_rows" | "year"
        | "month" | "day" | "hour" | "minute" | "second" | "unix_timestamp" | "datediff"
        | "sign" | "array_length" | "cardinality" => DataType::BigInt,
        "now"
        | "current_timestamp"
        | "transaction_timestamp"
        | "statement_timestamp"
        | "clock_timestamp"
        | "to_timestamp" => {
            if mysql {
                DataType::Timestamp
            } else {
                DataType::TimestampTz
            }
        }
        "localtimestamp" | "utc_timestamp" | "sysdate" | "getdate" | "date_trunc"
        | "from_unixtime" | "date_add" | "date_sub" | "convert_tz" => DataType::Timestamp,
        "current_date" | "curdate" | "date" | "to_date" | "make_date" => DataType::Date,
        "current_time" | "curtime" => DataType::Time,
        "gen_random_uuid" | "uuid_generate_v4" | "newid" => DataType::Uuid,
        "uuid" if mysql => DataType::Varchar(Some(36)),
        "random" | "rand" | "sqrt" | "power" | "pow" | "exp" | "ln" | "log" | "log10" | "pi" => {
            DataType::Double
        }
        "to_json" | "to_jsonb" | "json_build_object" | "jsonb_build_object"
        | "json_build_array" | "json_agg" | "jsonb_agg" | "json_object" | "json_array" => {
            DataType::Json
        }
        "bool_and"
        | "bool_or"
        | "every"
        | "pg_table_is_visible"
        | "has_table_privilege"
        | "starts_with"
        | "pg_is_in_recovery" => DataType::Bool,
        "abs" | "round" | "ceil" | "ceiling" | "floor" | "trunc" | "mod" => return arg_ty(0),
        "array_agg" => DataType::Array(Box::new(arg_ty(0).unwrap_or(DataType::Text))),
        "date_part" | "extract" => DataType::Numeric(None),
        _ => {
            let _ = star;
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_helpers() {
        let j = r#"{"a": {"b": [1, "x", null]}, "n": 1.5}"#;
        assert_eq!(
            json_get(j, &Datum::Text("n".into()), true).unwrap(),
            Datum::Text("1.5".into())
        );
        assert_eq!(json_path(j, "$.a.b[1]").unwrap().as_deref(), Some("\"x\""));
        assert_eq!(json_path(j, "$.zzz").unwrap(), None);
        assert!(json_contains(j, r#"{"a": {"b": [1]}}"#).unwrap());
        assert!(!json_contains(j, r#"{"n": 2}"#).unwrap());
        assert_eq!(
            json_concat(r#"{"a":1}"#, r#"{"b":2}"#).unwrap(),
            r#"{"a":1,"b":2}"#
        );
        assert!(parse_json("{bad}").is_err());
    }

    #[test]
    fn dates() {
        let ts = datum::parse_timestamp("2024-05-15 13:45:30.25").unwrap();
        assert_eq!(
            date_trunc("month", &Datum::Timestamp(ts))
                .unwrap()
                .as_text(),
            "2024-05-01 00:00:00"
        );
        assert_eq!(
            extract("dow", &Datum::Timestamp(ts)).unwrap(),
            Datum::Int(3)
        );
        assert_eq!(
            extract("second", &Datum::Timestamp(ts)).unwrap().as_text(),
            "30.25"
        );
        assert_eq!(
            extract("week", &Datum::Timestamp(ts)).unwrap(),
            Datum::Int(20)
        );
        assert_eq!(
            format_datetime(&Datum::Timestamp(ts), "YYYY-MM-DD HH24:MI", false).unwrap(),
            "2024-05-15 13:45"
        );
        assert_eq!(
            format_datetime(&Datum::Timestamp(ts), "%Y/%m/%d %H:%i:%s", true).unwrap(),
            "2024/05/15 13:45:30"
        );
    }
}
