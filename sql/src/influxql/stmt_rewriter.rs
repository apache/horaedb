// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql statement rewriter

use std::{collections::BTreeSet, ops::ControlFlow};

use common_util::error::BoxError;
use influxdb_influxql_parser::{
    common::{MeasurementName, QualifiedMeasurementName, ZeroOrMore},
    expression::{walk, Expr, WildcardType},
    identifier::Identifier,
    literal::Literal,
    select::{
        Dimension, Field, FieldList, FromMeasurementClause, MeasurementSelection, SelectStatement,
    },
};
use itertools::{Either, Itertools};
use snafu::{ensure, OptionExt, ResultExt};

use super::{planner::MeasurementProvider, util};
use crate::influxql::error::*;

/// Rewriter for the influxql statement
///
/// It will rewrite statement before converting it to sql statement.
// Partial copy from influxdb_iox.
pub(crate) struct StmtRewriter<'a> {
    measurement_provider: &'a dyn MeasurementProvider,
}

impl<'a> StmtRewriter<'a> {
    #[allow(dead_code)]
    pub fn new(measurement_provider: &'a dyn MeasurementProvider) -> Self {
        Self {
            measurement_provider,
        }
    }

    #[allow(dead_code)]
    pub fn rewrite(&self, stmt: &mut SelectStatement) -> Result<()> {
        self.rewrite_from(stmt)?;
        self.rewrite_field_list(stmt)
    }

    fn rewrite_from(&self, stmt: &mut SelectStatement) -> Result<()> {
        let mut new_from = Vec::new();
        for ms in stmt.from.iter() {
            match ms {
                MeasurementSelection::Name(qmn) => match qmn {
                    QualifiedMeasurementName {
                        name: MeasurementName::Name(name),
                        ..
                    } => {
                        let _ = self.measurement_provider.measurement(name)?.context(
                            RewriteNoCause {
                                msg: format!("measurement not found, measurement:{name}"),
                            },
                        )?;
                        new_from.push(ms.clone());
                    }
                    QualifiedMeasurementName {
                        name: MeasurementName::Regex(_),
                        ..
                    } => {
                        // TODO: need to support get all tables first.
                        return Unimplemented {
                            msg: "rewrite from regex",
                        }
                        .fail();
                    }
                },
                MeasurementSelection::Subquery(_) => {
                    return Unimplemented {
                        msg: "rewrite from subquery",
                    }
                    .fail();
                }
            }
        }

        // TODO: support from multiple tables.
        ensure!(
            new_from.len() == 1,
            Unimplemented {
                msg: "rewrite from multiple measurements"
            }
        );

        stmt.from = FromMeasurementClause::new(new_from);

        Ok(())
    }

    /// Rewrite the projection list and GROUP BY of the specified `SELECT`.
    // TODO: should support from multiple measurements.
    // TODO: support rewrite fields in subquery.
    fn rewrite_field_list(&self, stmt: &mut SelectStatement) -> Result<()> {
        ensure!(
            stmt.from.len() == 1,
            Unimplemented {
                msg: "rewrite field list from multiple measurements"
            }
        );

        match &stmt.from[0] {
            MeasurementSelection::Name(qualified_name) => {
                let QualifiedMeasurementName { name, .. } = qualified_name;

                match name {
                    MeasurementName::Name(name) => {
                        // Get schema, and split columns to tags and fields.
                        let (tags, fields) = self
                            .tags_and_fields_in_measurement(name.as_str())
                            .box_err()
                            .context(RewriteWithCause {
                                msg: "rewrite field list fail to find measurement",
                            })?;
                        let mut group_by_tags = BTreeSet::new();
                        maybe_rewrite_group_by(&tags, &mut group_by_tags, stmt)?;
                        maybe_rewrite_projection(&tags, &fields, &group_by_tags, stmt)?;

                        Ok(())
                    }

                    MeasurementName::Regex(_) => RewriteNoCause {
                        msg: "rewrite field list should not encounter regex in from clause",
                    }
                    .fail(),
                }
            }

            MeasurementSelection::Subquery(_) => Unimplemented {
                msg: "rewrite field list from subquery",
            }
            .fail(),
        }
    }

    fn tags_and_fields_in_measurement(
        &self,
        measurement_name: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let measurement = self
            .measurement_provider
            .measurement(measurement_name)
            .box_err()
            .context(RewriteWithCause {
                msg: format!("failed to find measurement, measurement:{measurement_name}"),
            })?
            .context(RewriteNoCause {
                msg: format!("measurement not found, measurement:{measurement_name}"),
            })?;

        // Get schema and split to tags and fields.
        let schema = measurement.schema();
        let tsid_idx_opt = schema.index_of_tsid();
        let timestamp_key_idx = schema.timestamp_index();
        let tags_and_fields: (Vec<String>, Vec<String>) = schema
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(col_idx, col)| {
                let is_tsid_col = match tsid_idx_opt {
                    Some(idx) => col_idx == idx,
                    None => false,
                };
                let is_timestamp_key_col = col_idx == timestamp_key_idx;

                if !is_tsid_col && !is_timestamp_key_col {
                    Some(col)
                } else {
                    None
                }
            })
            .partition_map(|col| {
                if col.is_tag {
                    Either::Left(col.name.clone())
                } else {
                    Either::Right(col.name.clone())
                }
            });
        Ok(tags_and_fields)
    }
}

fn maybe_rewrite_group_by(
    tags: &[String],
    group_by_tags: &mut BTreeSet<String>,
    stmt: &mut SelectStatement,
) -> Result<()> {
    if let Some(group_by) = &stmt.group_by {
        for dimension in group_by.iter() {
            match dimension {
                Dimension::Time { .. } => {
                    return Unimplemented {
                        msg: "group by time interval",
                    }
                    .fail();
                }

                Dimension::Tag(tag) => {
                    if !tags.contains(&tag.to_string()) {
                        return RewriteNoCause {
                            msg: format!("rewrite group by encounter tag not exist, tag:{tag}, exist tags:{tags:?}"),
                        }
                        .fail();
                    }
                    let _ = group_by_tags.insert(tag.to_string());
                }

                Dimension::Regex(re) => {
                    let re = util::parse_regex(re).box_err().context(RewriteWithCause {
                        msg: format!("rewrite group by encounter invalid regex, regex:{re}"),
                    })?;
                    let match_tags = tags.iter().filter_map(|tag| {
                        if re.is_match(tag.as_str()) {
                            Some(tag.clone())
                        } else {
                            None
                        }
                    });
                    group_by_tags.extend(match_tags);
                }

                Dimension::Wildcard => group_by_tags.extend(tags.iter().cloned()),
            }
        }

        stmt.group_by = Some(ZeroOrMore::new(
            group_by_tags
                .iter()
                .map(|tag| Dimension::Tag(Identifier::new(tag.clone())))
                .collect::<Vec<_>>(),
        ));
    }

    Ok(())
}

fn maybe_rewrite_projection(
    tags: &[String],
    fields: &[String],
    groub_by_tags: &BTreeSet<String>,
    stmt: &mut SelectStatement,
) -> Result<()> {
    let mut new_fields = Vec::new();

    enum AddFieldType {
        Tag,
        Field,
        Both,
    }

    let add_fields = |filter: &dyn Fn(&String) -> bool,
                      add_field_type: AddFieldType,
                      new_fields: &mut Vec<Field>| {
        if matches!(&add_field_type, AddFieldType::Tag | AddFieldType::Both) {
            let tag_fields = tags.iter().filter_map(|tag| {
                if !groub_by_tags.contains(tag.as_str()) && filter(tag) {
                    Some(Field {
                        expr: Expr::VarRef {
                            name: tag.clone().into(),
                            data_type: None,
                        },
                        alias: None,
                    })
                } else {
                    None
                }
            });
            new_fields.extend(tag_fields);
        }

        if matches!(&add_field_type, AddFieldType::Field | AddFieldType::Both) {
            let normal_fields = fields.iter().filter_map(|field| {
                if filter(field) {
                    Some(Field {
                        expr: Expr::VarRef {
                            name: field.clone().into(),
                            data_type: None,
                        },
                        alias: None,
                    })
                } else {
                    None
                }
            });
            new_fields.extend(normal_fields);
        }
    };

    for f in stmt.fields.iter() {
        match &f.expr {
            Expr::Wildcard(wct) => {
                let filter = |_: &String| -> bool { true };

                match wct {
                    Some(WildcardType::Tag) => {
                        add_fields(&filter, AddFieldType::Tag, &mut new_fields);
                    }
                    Some(WildcardType::Field) => {
                        add_fields(&filter, AddFieldType::Field, &mut new_fields);
                    }
                    None => {
                        add_fields(&filter, AddFieldType::Both, &mut new_fields);
                    }
                }
            }

            Expr::Literal(Literal::Regex(re)) => {
                let re = util::parse_regex(re).box_err().context(RewriteWithCause {
                    msg: format!("rewrite projection encounter invalid regex, regex:{re}"),
                })?;

                let filter = |v: &String| -> bool { re.is_match(v.as_str()) };

                add_fields(&filter, AddFieldType::Both, &mut new_fields);
            }

            Expr::Call { args, .. } => {
                let mut args = args;

                // Search for the call with a wildcard by continuously descending until
                // we no longer have a call.
                while let Some(Expr::Call {
                    args: inner_args, ..
                }) = args.first()
                {
                    args = inner_args;
                }

                match args.first() {
                    Some(Expr::Wildcard(Some(WildcardType::Tag))) => {
                        return RewriteNoCause {
                            msg: "rewrite projection found tags placed in a call",
                        }
                        .fail();
                    }
                    Some(Expr::Wildcard(_)) | Some(Expr::Literal(Literal::Regex(_))) => {
                        return Unimplemented {
                            msg: "wildcard or regex in call",
                        }
                        .fail();
                    }
                    _ => {
                        new_fields.push(f.clone());
                        continue;
                    }
                }
            }

            Expr::Binary { .. } => {
                let has_wildcard = walk::walk_expr(&f.expr, &mut |e| {
                    match e {
                        Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_)) => {
                            return ControlFlow::Break(())
                        }
                        _ => {}
                    }
                    ControlFlow::Continue(())
                })
                .is_break();

                if has_wildcard {
                    return RewriteNoCause {
                        msg: "rewrite projection encounter wildcard or regex in binary expression",
                    }
                    .fail();
                }

                new_fields.push(f.clone());
            }

            _ => new_fields.push(f.clone()),
        }
    }

    stmt.fields = FieldList::new(new_fields);

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        influxql::test_util::{parse_select, rewrite_statement},
        tests::MockMetaProvider,
    };

    #[test]
    fn test_wildcard_and_regex_in_projection() {
        let namespace = MockMetaProvider::default();

        let mut stmt = parse_select("SELECT * FROM influxql_test");
        rewrite_statement(&namespace, &mut stmt);
        assert_eq!(
            "SELECT col1, col2, col3 FROM influxql_test",
            stmt.to_string()
        );

        let mut stmt = parse_select("SELECT *::tag FROM influxql_test");
        rewrite_statement(&namespace, &mut stmt);
        assert_eq!("SELECT col1, col2 FROM influxql_test", stmt.to_string());

        let mut stmt = parse_select("SELECT *::field FROM influxql_test");
        rewrite_statement(&namespace, &mut stmt);
        assert_eq!("SELECT col3 FROM influxql_test", stmt.to_string());
    }

    #[test]
    fn test_wildcard_and_regex_in_group_by() {
        let namespace = MockMetaProvider::default();

        let mut stmt = parse_select("SELECT * FROM influxql_test GROUP BY *");
        rewrite_statement(&namespace, &mut stmt);
        assert_eq!(
            "SELECT col3 FROM influxql_test GROUP BY col1, col2",
            stmt.to_string()
        );

        let mut stmt = parse_select("SELECT * FROM influxql_test GROUP BY col1");
        rewrite_statement(&namespace, &mut stmt);
        assert_eq!(
            "SELECT col2, col3 FROM influxql_test GROUP BY col1",
            stmt.to_string()
        );
    }
}
