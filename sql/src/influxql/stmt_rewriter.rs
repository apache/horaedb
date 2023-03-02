use common_util::error::BoxError;
use influxdb_influxql_parser::{
    common::{MeasurementName, QualifiedMeasurementName},
    expression::Expr,
    literal::Literal,
    select::{Dimension, FromMeasurementClause, MeasurementSelection, SelectStatement},
};
use snafu::ResultExt;

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
                        let table = self
                            .sql_planner
                            .find_table(name)
                            .box_err()
                            .context(RewriteStmtWithCause)?;
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
                            msg: "regex in from clause",
                        }
                        .fail();
                    }
                },
                MeasurementSelection::Subquery(_) => {
                    return Unimplemented {
                        msg: "subquery in from clause",
                    }
                    .fail();
                }
            }
        }
        stmt.from = FromMeasurementClause::new(new_from);

        Ok(())
    }

    /// Rewrite the projection list and GROUP BY of the specified `SELECT`
    /// statement.
    ///
    /// Wildcards and regular expressions in the `SELECT` projection list and
    /// `GROUP BY` are expanded. Any fields with no type specifier are
    /// rewritten with the appropriate type, if they exist in the underlying
    /// schema.
    ///
    /// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L1185).
    fn rewrite_field_list(&self, stmt: &mut SelectStatement) -> Result<()> {
        // TODO: support rewrite fields in subquery.

        // Attempt to rewrite all variable references in the fields with their types, if
        // one hasn't been specified.
        if let ControlFlow::Break(e) = stmt.fields.iter_mut().try_for_each(|f| {
            walk_expr_mut::<DataFusionError>(&mut f.expr, &mut |e| {
                if matches!(e, Expr::VarRef { .. }) {
                    let new_type = match evaluate_type(s, e.borrow(), &stmt.from) {
                        Err(e) => ControlFlow::Break(e)?,
                        Ok(v) => v,
                    };

                    if let Expr::VarRef { data_type, .. } = e {
                        *data_type = new_type;
                    }
                }
                ControlFlow::Continue(())
            })
        }) {
            return Err(e);
        }

        let (has_field_wildcard, has_group_by_wildcard) = has_wildcards(stmt);
        if (has_field_wildcard, has_group_by_wildcard) == (false, false) {
            return Ok(());
        }

        let (field_set, mut tag_set) = from_field_and_dimensions(s, &stmt.from)?;

        if !has_group_by_wildcard {
            if let Some(group_by) = &stmt.group_by {
                // Remove any explicitly listed tags in the GROUP BY clause, so they are not
                // expanded in the wildcard specified in the SELECT projection
                // list
                group_by.iter().for_each(|dim| {
                    if let Dimension::Tag(ident) = dim {
                        tag_set.remove(ident.as_str());
                    }
                });
            }
        }

        #[derive(PartialEq, PartialOrd, Eq, Ord)]
        struct VarRef {
            name: String,
            data_type: VarRefDataType,
        }

        let fields = if !field_set.is_empty() {
            let fields_iter = field_set.iter().map(|(k, v)| VarRef {
                name: k.clone(),
                data_type: *v,
            });

            if !has_group_by_wildcard {
                fields_iter
                    .chain(tag_set.iter().map(|tag| VarRef {
                        name: tag.clone(),
                        data_type: VarRefDataType::Tag,
                    }))
                    .sorted()
                    .collect::<Vec<_>>()
            } else {
                fields_iter.sorted().collect::<Vec<_>>()
            }
        } else {
            vec![]
        };

        if has_field_wildcard {
            let mut new_fields = Vec::new();

            for f in stmt.fields.iter() {
                let add_field = |f: &VarRef| {
                    new_fields.push(Field {
                        expr: Expr::VarRef {
                            name: f.name.clone().into(),
                            data_type: Some(f.data_type),
                        },
                        alias: None,
                    })
                };

                match &f.expr {
                    Expr::Wildcard(wct) => {
                        let filter: fn(&&VarRef) -> bool = match wct {
                            None => |_| true,
                            Some(WildcardType::Tag) => |v| v.data_type.is_tag_type(),
                            Some(WildcardType::Field) => |v| v.data_type.is_field_type(),
                        };

                        fields.iter().filter(filter).for_each(add_field);
                    }

                    Expr::Literal(Literal::Regex(re)) => {
                        let re = util::parse_regex(re)?;
                        fields
                            .iter()
                            .filter(|v| re.is_match(v.name.as_str()))
                            .for_each(add_field);
                    }

                    Expr::Call { name, args } => {
                        let mut name = name;
                        let mut args = args;

                        // Search for the call with a wildcard by continuously descending until
                        // we no longer have a call.
                        while let Some(Expr::Call {
                            name: inner_name,
                            args: inner_args,
                        }) = args.first()
                        {
                            name = inner_name;
                            args = inner_args;
                        }

                        // Add additional types for certain functions.
                        match name.to_lowercase().as_str() {
                            "count" | "first" | "last" | "distinct" | "elapsed" | "mode"
                            | "sample" => {
                                supported_types
                                    .extend([VarRefDataType::String, VarRefDataType::Boolean]);
                            }
                            "min" | "max" => {
                                supported_types.insert(VarRefDataType::Boolean);
                            }
                            "holt_winters" | "holt_winters_with_fit" => {
                                supported_types.remove(&VarRefDataType::Unsigned);
                            }
                            _ => {}
                        }

                        let add_field = |v: &VarRef| {
                            let mut args = args.clone();
                            args[0] = Expr::VarRef {
                                name: v.name.clone().into(),
                                data_type: Some(v.data_type),
                            };
                            new_fields.push(Field {
                                expr: Expr::Call {
                                    name: name.clone(),
                                    args,
                                },
                                alias: Some(format!("{}_{}", field_name(f), v.name).into()),
                            })
                        };

                        match args.first() {
                            Some(Expr::Wildcard(Some(WildcardType::Tag))) => {
                                return Err(DataFusionError::External(
                                    format!("unable to use tag as wildcard in {name}()").into(),
                                ));
                            }
                            Some(Expr::Wildcard(_)) => {
                                fields
                                    .iter()
                                    .filter(|v| supported_types.contains(&v.data_type))
                                    .for_each(add_field);
                            }
                            Some(Expr::Literal(Literal::Regex(re))) => {
                                let re = util::parse_regex(re)?;
                                fields
                                    .iter()
                                    .filter(|v| {
                                        supported_types.contains(&v.data_type)
                                            && re.is_match(v.name.as_str())
                                    })
                                    .for_each(add_field);
                            }
                            _ => {
                                new_fields.push(f.clone());
                                continue;
                            }
                        }
                    }

                    Expr::Binary { .. } => {
                        let has_wildcard = walk_expr(&f.expr, &mut |e| {
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
                            return Err(DataFusionError::External(
                                "unsupported expression: contains a wildcard or regular expression"
                                    .into(),
                            ));
                        }

                        new_fields.push(f.clone());
                    }

                    _ => new_fields.push(f.clone()),
                }
            }

            stmt.fields = FieldList::new(new_fields);
        }

        // group by 展开
        if has_group_by_wildcard {
            let group_by_tags = if has_group_by_wildcard {
                tag_set.into_iter().sorted().collect::<Vec<_>>()
            } else {
                vec![]
            };

            if let Some(group_by) = &stmt.group_by {
                let mut new_dimensions = Vec::new();

                for dim in group_by.iter() {
                    let add_dim = |dim: &String| {
                        new_dimensions.push(Dimension::Tag(Identifier::new(dim.clone())))
                    };

                    match dim {
                        Dimension::Wildcard => {
                            group_by_tags.iter().for_each(add_dim);
                        }
                        Dimension::Regex(re) => {
                            let re = util::parse_regex(re)?;

                            group_by_tags
                                .iter()
                                .filter(|dim| re.is_match(dim.as_str()))
                                .for_each(add_dim);
                        }
                        _ => new_dimensions.push(dim.clone()),
                    }
                }
                stmt.group_by = Some(GroupByClause::new(new_dimensions));
            }
        }

        Ok(())
    }

    /// Determine the merged fields and tags of the `FROM` clause.
    fn from_field_and_dimensions(
        s: &dyn SchemaProvider,
        from: &FromMeasurementClause,
    ) -> Result<(FieldTypeMap, TagSet)> {
        let mut fs = FieldTypeMap::new();
        let mut ts = TagSet::new();

        for ms in from.deref() {
            match ms {
                MeasurementSelection::Name(QualifiedMeasurementName {
                    name: MeasurementName::Name(name),
                    ..
                }) => {
                    let (field_set, tag_set) = match field_and_dimensions(s, name.as_str())? {
                        Some(res) => res,
                        None => continue,
                    };

                    // Merge field_set with existing
                    for (name, ft) in &field_set {
                        match fs.get(name) {
                            Some(existing_type) => {
                                if ft < existing_type {
                                    fs.insert(name.to_string(), *ft);
                                }
                            }
                            None => {
                                fs.insert(name.to_string(), *ft);
                            }
                        };
                    }

                    ts.extend(tag_set);
                }
                MeasurementSelection::Subquery(select) => {
                    for f in select.fields.iter() {
                        let dt = match evaluate_type(s, &f.expr, &select.from)? {
                            Some(dt) => dt,
                            None => continue,
                        };

                        let name = field_name(f);

                        match fs.get(name.as_str()) {
                            Some(existing_type) => {
                                if dt < *existing_type {
                                    fs.insert(name, dt);
                                }
                            }
                            None => {
                                fs.insert(name, dt);
                            }
                        }
                    }

                    if let Some(group_by) = &select.group_by {
                        // Merge the dimensions from the subquery
                        ts.extend(group_by.iter().filter_map(|d| match d {
                            Dimension::Tag(ident) => Some(ident.to_string()),
                            _ => None,
                        }));
                    }
                }
                _ => {
                    // Unreachable, as the from clause should be normalised at this point.
                    return Err(DataFusionError::Internal(
                        "Unexpected MeasurementSelection in from".to_string(),
                    ));
                }
            }
        }
        Ok((fs, ts))
    }

    /// Returns a tuple indicating whether the specifies `SELECT` statement
    /// has any wildcards or regular expressions in the projection list
    /// and `GROUP BY` clause respectively.
    fn has_wildcards(stmt: &SelectStatement) -> (bool, bool) {
        use influxdb_influxql_parser::visit::{Recursion, Visitable, Visitor};

        struct HasWildcardsVisitor(bool, bool);

        impl Visitor for HasWildcardsVisitor {
            type Error = DataFusionError;

            fn pre_visit_expr(self, n: &Expr) -> Result<Recursion<Self>> {
                Ok(
                    if matches!(n, Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_))) {
                        Recursion::Stop(Self(true, self.1))
                    } else {
                        Recursion::Continue(self)
                    },
                )
            }

            fn pre_visit_select_from_clause(
                self,
                _n: &FromMeasurementClause,
            ) -> Result<Recursion<Self>> {
                // Don't traverse FROM and potential subqueries
                Ok(Recursion::Stop(self))
            }

            fn pre_visit_select_dimension(self, n: &Dimension) -> Result<Recursion<Self>> {
                Ok(if matches!(n, Dimension::Wildcard | Dimension::Regex(_)) {
                    Recursion::Stop(Self(self.0, true))
                } else {
                    Recursion::Continue(self)
                })
            }
        }

        let res = Visitable::accept(stmt, HasWildcardsVisitor(false, false)).unwrap();
        (res.0, res.1)
    }
}
