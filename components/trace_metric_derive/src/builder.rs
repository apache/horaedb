// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Field, Generics, Ident};

const COLLECTOR_FIELD_TOKENS: &str = "(collector)";
const COUNTER_FIELD_TOKENS: &str = "(counter)";
const ELAPSED_FIELD_TOKENS: &str = "(elapsed)";
const BOOLEAN_FIELD_TOKENS: &str = "(boolean)";

enum MetricType {
    Counter,
    Elapsed,
    Boolean,
}

impl MetricType {
    fn try_from_tokens(s: &str) -> Option<Self> {
        if s == COUNTER_FIELD_TOKENS {
            Some(Self::Counter)
        } else if s == ELAPSED_FIELD_TOKENS {
            Some(Self::Elapsed)
        } else if s == BOOLEAN_FIELD_TOKENS {
            Some(Self::Boolean)
        } else {
            None
        }
    }
}

struct MetricField {
    metric_type: MetricType,
    field_name: Ident,
}

impl MetricField {
    fn try_from_field(field: Field) -> Option<Self> {
        for attr in field.attrs.iter() {
            if !attr.path.is_ident("metric") {
                continue;
            }

            let field_name = field.ident.expect("Metric field must have a name");
            let metric_type_tokens = attr.tokens.to_string();
            let metric_type =
                MetricType::try_from_tokens(&metric_type_tokens).expect("Unknown metric type");
            return Some(Self {
                metric_type,
                field_name,
            });
        }

        None
    }
}

pub struct Builder {
    struct_name: Ident,
    metric_fields: Vec<MetricField>,
    collector_field: Ident,
    generics: Generics,
}

impl Builder {
    pub fn parse_from_ast(ast: DeriveInput) -> Self {
        let struct_name = ast.ident;
        let (metric_fields, collector_field) = match ast.data {
            syn::Data::Struct(syn::DataStruct {
                fields: syn::Fields::Named(syn::FieldsNamed { named, .. }),
                ..
            }) => {
                let mut metric_fields = Vec::new();
                let mut collector_field = None;
                for field in named {
                    if Self::is_collector_field(&field) {
                        collector_field = Some(field);
                        continue;
                    }
                    if let Some(metric_field) = MetricField::try_from_field(field) {
                        metric_fields.push(metric_field);
                    }
                }
                (
                    metric_fields,
                    collector_field
                        .expect("TracedMetrics must have a collector field")
                        .ident
                        .expect("TracedMetrics collector field must be named"),
                )
            }
            _ => panic!("TracedMetrics only supports struct with named fields"),
        };

        Self {
            struct_name,
            metric_fields,
            collector_field,
            generics: ast.generics,
        }
    }

    pub fn build(&self) -> TokenStream {
        let mut collect_statements = Vec::with_capacity(self.metric_fields.len());
        let collector_field = &self.collector_field;
        for metric_field in self.metric_fields.iter() {
            let field_name = &metric_field.field_name;
            let metric = match metric_field.metric_type {
                MetricType::Counter => {
                    quote! { trace_metric::Metric::counter("#field_name".to_string(), self.#field_name) }
                }
                MetricType::Elapsed => {
                    quote! { trace_metric::Metric::elapsed("#field_name".to_string(), self.#field_name) }
                }
                MetricType::Boolean => {
                    quote! { trace_metric::Metric::boolean("#field_name".to_string(), self.#field_name) }
                }
            };
            let statement = quote! {
                self.#collector_field.collect(#metric);
            };
            collect_statements.push(statement);
        }

        let where_clause = &self.generics.where_clause;
        let generics = &self.generics;
        let struct_name = &self.struct_name;
        quote! {
            impl #generics Drop for #struct_name #generics #where_clause {
                fn drop(&mut self) {
                    #(#collect_statements)*
                }
            }
        }
        .into()
    }

    fn is_collector_field(field: &Field) -> bool {
        field.attrs.iter().any(|attr| {
            attr.path.is_ident("metric")
                && attr.tokens.to_string().as_str() == COLLECTOR_FIELD_TOKENS
        })
    }
}
