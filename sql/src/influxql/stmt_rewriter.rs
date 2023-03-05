use std::{collections::HashSet, ops::ControlFlow};

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

use super::util;
use crate::{influxql::error::*, provider::MetaProvider};

pub(crate) struct StmtRewriter<'a, P: MetaProvider> {
    sql_planner: &'a crate::planner::PlannerDelegate<'a, P>,
}

// Partial copy from influxdb_iox.
impl<'a, P: MetaProvider> StmtRewriter<'a, P> {
    pub fn new(sql_planner: &'a crate::planner::PlannerDelegate<'a, P>) -> Self {
        Self { sql_planner }
    }

    pub fn rewrite_from(&self, stmt: &mut SelectStatement) -> Result<()> {
        let mut new_from = Vec::new();
        for ms in stmt.from.iter() {
            match ms {
                MeasurementSelection::Name(qmn) => match qmn {
                    QualifiedMeasurementName {
                        name: MeasurementName::Name(name),
                        ..
                    } => {
                        let table = self.sql_planner.find_table(name).box_err().context(
                            RewriteFromWithCause {
                                msg: format!("measurement not found, measurement:{name}"),
                            },
                        )?;
                        if table.is_some() {
                            new_from.push(ms.clone())
                        }
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
                        let (tags, fields) = self.tags_and_fields_in_measurement(name.as_str())?;
                        let mut group_by_tags = HashSet::new();
                        maybe_rewrite_group_by(&tags, &mut group_by_tags, stmt)?;
                        maybe_rewrite_projection(&tags, &fields, &group_by_tags, stmt)?;

                        Ok(())
                    }

                    MeasurementName::Regex(_) => RewriteFieldsNoCause {
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

    // TODO: just support from one table now.
    fn tags_and_fields_in_measurement(
        &self,
        measurement_name: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let measurement = self
            .sql_planner
            .find_table(measurement_name)
            .box_err()
            .context(RewriteFieldsWithCause {
                msg: format!("failed to find measurement, measurement:{measurement_name}"),
            })?
            .context(RewriteFieldsNoCause {
                msg: format!("measurement not found, measurement:{measurement_name}"),
            })?;

        // Get schema and split to tags and fields.
        let schema = measurement.schema();
        let tags_and_fields: (Vec<String>, Vec<String>) =
            schema.columns().iter().partition_map(|column| {
                if column.is_tag {
                    Either::Left(column.name.clone())
                } else {
                    Either::Right(column.name.clone())
                }
            });

        Ok(tags_and_fields)
    }
}

fn maybe_rewrite_group_by(
    tags: &[String],
    group_by_tags: &mut HashSet<String>,
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
                    if tags.contains(&tag.to_string()) {
                        return RewriteFieldsNoCause {
                            msg: format!("group by tag not exist, tag:{tag}, exist tags:{tags:?}"),
                        }
                        .fail();
                    }
                    let _ = group_by_tags.insert(tag.to_string());
                }

                Dimension::Regex(re) => {
                    let re = util::parse_regex(re)
                        .box_err()
                        .context(RewriteFieldsWithCause {
                            msg: format!("group by invalid regex, regex:{re}"),
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
    groub_by_tags: &HashSet<String>,
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
                let re = util::parse_regex(re)
                    .box_err()
                    .context(RewriteFieldsWithCause {
                        msg: format!("rewrite field list encounter invalid regex, regex:{re}"),
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
                        return RewriteFieldsNoCause {
                            msg: "tags can't be placed in a call",
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
                    return RewriteFieldsNoCause {
                        msg: "wildcard or regex should be encountered in binary expression",
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
