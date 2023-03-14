// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{DeriveInput, Field, Generics, Ident};

const COLLECTOR_FIELD_TOKENS: &str = "(collector)";
const NUMBER_FIELD_TOKENS: &str = "(number)";
const DURATION_FIELD_TOKENS: &str = "(duration)";
const BOOLEAN_FIELD_TOKENS: &str = "(boolean)";

enum MetricType {
    Number,
    Duration,
    Boolean,
}

impl MetricType {
    fn try_from_tokens(s: &str) -> Option<Self> {
        if s == NUMBER_FIELD_TOKENS {
            Some(Self::Number)
        } else if s == DURATION_FIELD_TOKENS {
            Some(Self::Duration)
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

struct CollectorField {
    field_name: Ident,
    optional: bool,
}

impl CollectorField {
    fn try_from_field(field: Field) -> Option<Self> {
        let is_collector_field = field.attrs.iter().any(|attr| {
            attr.path.is_ident("metric")
                && attr.tokens.to_string().as_str() == COLLECTOR_FIELD_TOKENS
        });

        if !is_collector_field {
            None
        } else {
            let ident = field.ident.expect("Collector field must be named");
            let type_tokens = field.ty.into_token_stream().to_string();
            Some(Self {
                field_name: ident,
                optional: type_tokens.starts_with("Option"),
            })
        }
    }
}

pub struct Builder {
    struct_name: Ident,
    metric_fields: Vec<MetricField>,
    collector_field: CollectorField,
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
                    if let Some(collector) = CollectorField::try_from_field(field.clone()) {
                        collector_field = Some(collector);
                    } else if let Some(metric_field) = MetricField::try_from_field(field) {
                        metric_fields.push(metric_field);
                    }
                }
                (
                    metric_fields,
                    collector_field.expect("TraceMetricWhenDrop must have a collector field"),
                )
            }
            _ => panic!("TraceMetricWhenDrop only supports struct with named fields"),
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
        for metric_field in self.metric_fields.iter() {
            let field_name = &metric_field.field_name;
            let metric = match metric_field.metric_type {
                MetricType::Number => {
                    quote! { ::trace_metric::Metric::number(stringify!(#field_name).to_string(), self.#field_name) }
                }
                MetricType::Duration => {
                    quote! { ::trace_metric::Metric::duration(stringify!(#field_name).to_string(), self.#field_name) }
                }
                MetricType::Boolean => {
                    quote! { ::trace_metric::Metric::boolean(stringify!(#field_name).to_string(), self.#field_name) }
                }
            };
            let statement = quote! {
                collector.collect(#metric);
            };
            collect_statements.push(statement);
        }

        let where_clause = &self.generics.where_clause;
        let generics = &self.generics;
        let struct_name = &self.struct_name;
        let collector_field_name = &self.collector_field.field_name;
        let stream = if self.collector_field.optional {
            quote! {
                impl #generics ::core::ops::Drop for #struct_name #generics #where_clause {
                    fn drop(&mut self) {
                        if let Some(collector) = &self.#collector_field_name {
                            #(#collect_statements)*
                        }
                    }
                }
            }
        } else {
            quote! {
                impl #generics ::core::ops::Drop for #struct_name #generics #where_clause {
                    fn drop(&mut self) {
                        let collector = &self.#collector_field_name;
                        #(#collect_statements)*
                    }
                }
            }
        };

        stream.into()
    }
}
