//! `MERGE`: each source row is matched against a snapshot of the target, and the chosen
//! action runs as an ordinary INSERT / UPDATE / DELETE (so constraints, versioned writes and
//! identity columns behave exactly as they do for those statements).

use crate::ast::{
    BinaryOp, Delete, Expr, Insert, InsertSource, Merge, MergeAction, MergeWhen, SelectItem,
    TableAlias, TableRef, Update,
};
use crate::datum::Datum;
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::{Env, Exec};
use crate::exec::select::ColMeta;
use crate::exec::{dml, OutCol, Outcome, ResultSet, Session};

/// What one action did, for OUTPUT: the row before and after, and the source row.
struct Change {
    action: &'static str,
    deleted: Option<Vec<Datum>>,
    inserted: Option<Vec<Datum>>,
    source: Vec<Datum>,
}

pub fn merge(s: &mut Session, m: &Merge, params: &[Datum]) -> SqlResult<Outcome> {
    let table = s.engine.table(&m.target)?;
    let qual = m.alias.clone().unwrap_or_else(|| m.target.clone());
    let (target, source) = {
        let ex = Exec::new(s, params);
        let target = ex.relation_of(
            &TableRef::Named {
                schema: None,
                name: m.target.clone(),
                alias: Some(TableAlias {
                    name: qual.clone(),
                    columns: Vec::new(),
                }),
            },
            None,
        )?;
        let source = ex.relation_of(&m.source, None)?;
        (target, source)
    };
    let mut cols: Vec<ColMeta> = target.cols.clone();
    cols.extend(source.cols.iter().cloned());
    let width = target.cols.len();
    let null_target = vec![Datum::Null; width];
    let null_source = vec![Datum::Null; source.cols.len()];
    let combined = |t: &[Datum], src: &[Datum]| {
        let mut r = t.to_vec();
        r.extend_from_slice(src);
        r
    };
    let pk = table.primary_key.clone();
    let mut matched = vec![false; target.rows.len()];
    let mut changes = Vec::new();

    for src in &source.rows {
        let mut hits = Vec::new();
        {
            let ex = Exec::new(s, params);
            for (i, t) in target.rows.iter().enumerate() {
                let row = combined(t, src);
                let env = Env {
                    cols: &cols,
                    row: &row,
                    outer: None,
                    aggs: None,
                };
                if ex.eval_bool(&m.on, &env)? {
                    hits.push(i);
                }
            }
        }
        if hits.is_empty() {
            let row = combined(&null_target, src);
            if let Some(c) = pick(s, m, MergeWhen::NotMatchedByTarget, &cols, &row, params)? {
                changes.extend(apply(s, m, &table, &pk, c, &cols, &row, None, src, params)?);
            }
            continue;
        }
        for i in hits {
            if matched[i] {
                return Err(SqlError::new(
                    "21000",
                    "The MERGE statement attempted to UPDATE or DELETE the same row more than once.",
                ));
            }
            matched[i] = true;
            let row = combined(&target.rows[i], src);
            if let Some(c) = pick(s, m, MergeWhen::Matched, &cols, &row, params)? {
                let old = target.rows[i].clone();
                changes.extend(apply(
                    s,
                    m,
                    &table,
                    &pk,
                    c,
                    &cols,
                    &row,
                    Some(old),
                    src,
                    params,
                )?);
            }
        }
    }
    if m.clauses
        .iter()
        .any(|c| c.when == MergeWhen::NotMatchedBySource)
    {
        for (i, t) in target.rows.iter().enumerate() {
            if matched[i] {
                continue;
            }
            let row = combined(t, &null_source);
            if let Some(c) = pick(s, m, MergeWhen::NotMatchedBySource, &cols, &row, params)? {
                changes.extend(apply(
                    s,
                    m,
                    &table,
                    &pk,
                    c,
                    &cols,
                    &row,
                    Some(t.clone()),
                    &null_source,
                    params,
                )?);
            }
        }
    }
    if m.output.is_empty() {
        let n = changes.len();
        return Ok(Outcome::Command {
            tag: format!("MERGE {n}"),
            affected: n as u64,
            last_insert_id: None,
        });
    }
    output(s, m, &table, &source.cols, &changes, params)
}

/// The first clause of kind `when` whose condition holds for `row`.
fn pick(
    s: &Session,
    m: &Merge,
    when: MergeWhen,
    cols: &[ColMeta],
    row: &[Datum],
    params: &[Datum],
) -> SqlResult<Option<usize>> {
    let ex = Exec::new(s, params);
    let env = Env {
        cols,
        row,
        outer: None,
        aggs: None,
    };
    for (k, c) in m.clauses.iter().enumerate() {
        if c.when != when {
            continue;
        }
        match &c.cond {
            Some(e) if !ex.eval_bool(e, &env)? => continue,
            _ => return Ok(Some(k)),
        }
    }
    Ok(None)
}

/// `pk = $n AND ...` for the target row `old`, with its values appended to `args`.
fn pk_filter(
    table: &crate::catalog::Table,
    pk: &[usize],
    old: &[Datum],
    args: &mut Vec<Datum>,
) -> SqlResult<Expr> {
    if pk.is_empty() {
        return Err(SqlError::unsupported(
            "MERGE ... UPDATE / DELETE on a table without a primary key",
        ));
    }
    let mut filter: Option<Expr> = None;
    for &k in pk {
        args.push(old[k].clone());
        let eq = Expr::binary(
            BinaryOp::Eq,
            Expr::Column(vec![table.columns[k].name.clone()]),
            Expr::Param(args.len() - 1),
        );
        filter = Some(match filter {
            None => eq,
            Some(f) => Expr::binary(BinaryOp::And, f, eq),
        });
    }
    Ok(filter.unwrap_or(Expr::Bool(true)))
}

#[allow(clippy::too_many_arguments)]
fn apply(
    s: &mut Session,
    m: &Merge,
    table: &crate::catalog::Table,
    pk: &[usize],
    clause: usize,
    cols: &[ColMeta],
    row: &[Datum],
    old: Option<Vec<Datum>>,
    src: &[Datum],
    params: &[Datum],
) -> SqlResult<Option<Change>> {
    let want_rows = !m.output.is_empty();
    let returning = if want_rows {
        vec![SelectItem::Wildcard(None)]
    } else {
        Vec::new()
    };
    let eval = |s: &Session, e: &Expr| -> SqlResult<Datum> {
        let ex = Exec::new(s, params);
        let env = Env {
            cols,
            row,
            outer: None,
            aggs: None,
        };
        ex.eval(e, &env)
    };
    let first_row = |out: Outcome| match out {
        Outcome::Rows(rs) => rs.rows.into_iter().next(),
        _ => None,
    };
    Ok(match &m.clauses[clause].action {
        MergeAction::DoNothing => None,
        MergeAction::Insert { columns, values } => {
            let mut args = Vec::new();
            let source = match values {
                None => InsertSource::DefaultValues,
                Some(vals) => {
                    let mut out = Vec::with_capacity(vals.len());
                    for v in vals {
                        if matches!(v, Expr::Default) {
                            out.push(Expr::Default);
                        } else {
                            args.push(eval(s, v)?);
                            out.push(Expr::Param(args.len() - 1));
                        }
                    }
                    InsertSource::Values(vec![out])
                }
            };
            let ins = Insert {
                table: m.target.clone(),
                alias: None,
                columns: columns.iter().map(|c| last_part(c)).collect(),
                source,
                on_conflict: None,
                returning,
                ignore: false,
                replace: false,
            };
            let out = dml::insert(s, &ins, &args)?;
            Some(Change {
                action: "INSERT",
                deleted: None,
                inserted: if want_rows { first_row(out) } else { None },
                source: src.to_vec(),
            })
        }
        MergeAction::Update(sets) => {
            let Some(old) = old else { return Ok(None) };
            let mut args = Vec::new();
            let mut assigned = Vec::with_capacity(sets.len());
            for (c, e) in sets {
                args.push(eval(s, e)?);
                assigned.push((last_part(c), Expr::Param(args.len() - 1)));
            }
            let where_ = pk_filter(table, pk, &old, &mut args)?;
            let upd = Update {
                table: m.target.clone(),
                alias: None,
                sets: assigned,
                from: Vec::new(),
                where_: Some(where_),
                returning,
                order_by: Vec::new(),
                limit: None,
            };
            let out = dml::update(s, &upd, &args)?;
            Some(Change {
                action: "UPDATE",
                deleted: Some(old),
                inserted: if want_rows { first_row(out) } else { None },
                source: src.to_vec(),
            })
        }
        MergeAction::Delete => {
            let Some(old) = old else { return Ok(None) };
            let mut args = Vec::new();
            let where_ = pk_filter(table, pk, &old, &mut args)?;
            let del = Delete {
                table: m.target.clone(),
                alias: None,
                using: Vec::new(),
                where_: Some(where_),
                returning: Vec::new(),
                order_by: Vec::new(),
                limit: None,
            };
            dml::delete(s, &del, &args)?;
            Some(Change {
                action: "DELETE",
                deleted: Some(old),
                inserted: None,
                source: src.to_vec(),
            })
        }
    })
}

/// `t.col` / `[col]` in a SET or INSERT column list: the column name.
fn last_part(c: &str) -> String {
    c.rsplit('.').next().unwrap_or(c).to_owned()
}

/// Evaluates the OUTPUT list over `inserted.*`, `deleted.*`, the source row and `$action`.
fn output(
    s: &Session,
    m: &Merge,
    table: &crate::catalog::Table,
    source_cols: &[ColMeta],
    changes: &[Change],
    params: &[Datum],
) -> SqlResult<Outcome> {
    let image = |qual: &str| -> Vec<ColMeta> {
        table
            .columns
            .iter()
            .map(|c| {
                let mut meta = ColMeta::new(c.name.clone(), Some(c.ty.clone()));
                meta.qual = Some(qual.to_owned());
                meta
            })
            .collect()
    };
    let mut cols = image("inserted");
    cols.extend(image("deleted"));
    cols.extend(source_cols.iter().cloned());
    cols.push(ColMeta::new("$action", Some(crate::ast::DataType::Text)));
    let n = table.columns.len();
    let ex = Exec::new(s, params);
    let mut out_cols: Vec<OutCol> = Vec::new();
    let mut rows = Vec::with_capacity(changes.len());
    for (k, ch) in changes.iter().enumerate() {
        let mut row = ch.inserted.clone().unwrap_or_else(|| vec![Datum::Null; n]);
        row.extend(ch.deleted.clone().unwrap_or_else(|| vec![Datum::Null; n]));
        row.extend(ch.source.iter().cloned());
        row.push(Datum::Text(ch.action.into()));
        let env = Env {
            cols: &cols,
            row: &row,
            outer: None,
            aggs: None,
        };
        let mut out = Vec::new();
        for item in &m.output {
            match item {
                SelectItem::Wildcard(q) => {
                    let q = q.as_deref().unwrap_or("inserted").to_ascii_lowercase();
                    for (i, c) in cols.iter().enumerate() {
                        if c.qual.as_deref().map(str::to_ascii_lowercase).as_deref() == Some(&q) {
                            out.push(row[i].clone());
                            if k == 0 {
                                out_cols.push(OutCol {
                                    name: c.name.clone(),
                                    ty: c.ty.clone().unwrap_or(crate::ast::DataType::Text),
                                    table: None,
                                });
                            }
                        }
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    out.push(ex.eval(expr, &env)?);
                    if k == 0 {
                        let name = alias.clone().unwrap_or_else(|| match expr {
                            Expr::Column(p) => p[p.len() - 1].clone(),
                            _ => String::new(),
                        });
                        let ty = ex
                            .static_type(expr, &cols)
                            .0
                            .unwrap_or(crate::ast::DataType::Text);
                        out_cols.push(OutCol {
                            name,
                            ty,
                            table: None,
                        });
                    }
                }
            }
        }
        rows.push(out);
    }
    if changes.is_empty() {
        // Column metadata for an empty result.
        for item in &m.output {
            if let SelectItem::Expr { expr, alias } = item {
                out_cols.push(OutCol {
                    name: alias.clone().unwrap_or_default(),
                    ty: ex
                        .static_type(expr, &cols)
                        .0
                        .unwrap_or(crate::ast::DataType::Text),
                    table: None,
                });
            }
        }
    }
    Ok(Outcome::Rows(ResultSet {
        columns: out_cols,
        rows,
    }))
}
