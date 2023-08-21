// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{DeriveInput, Field, Generics, Ident};

const COLLECTOR_FIELD_TOKENS: &str = "(collector)";
const NUMBER_FIELD_TOKENS: &str = "number";
const DURATION_FIELD_TOKENS: &str = "duration";
const BOOLEAN_FIELD_TOKENS: &str = "boolean";

#[derive(Debug, Clone)]
enum MetricAggregator {
    Sum,
}

impl ToTokens for MetricAggregator {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.append(Ident::new(&format!("{self:?}"), Span::call_site()));
    }
}

#[derive(Debug)]
enum MetricType {
    Number,
    Duration,
    Boolean,
}

impl ToTokens for MetricType {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.append(Ident::new(
            &format!("{self:?}").to_lowercase(),
            Span::call_site(),
        ));
    }
}

struct MetricMetadata {
    typ: MetricType,
    aggregator: Option<MetricAggregator>,
}

impl MetricMetadata {
    fn parse_aggregator(s: &str) -> Option<MetricAggregator> {
        match s.to_lowercase().as_str() {
            "sum" => Some(MetricAggregator::Sum),
            _ => None,
        }
    }

    fn parse_type(s: &str) -> Option<MetricType> {
        if s == NUMBER_FIELD_TOKENS {
            Some(MetricType::Number)
        } else if s == DURATION_FIELD_TOKENS {
            Some(MetricType::Duration)
        } else if s == BOOLEAN_FIELD_TOKENS {
            Some(MetricType::Boolean)
        } else {
            None
        }
    }

    fn try_from_tokens(tokens: &proc_macro2::TokenStream) -> Option<Self> {
        for tree in tokens.clone().into_iter() {
            if let proc_macro2::TokenTree::Group(group) = tree {
                let trees = group.stream().into_iter().collect::<Vec<_>>();
                match trees.len() {
                    // #[metric(number)]
                    1 => {
                        return Self::parse_type(&trees[0].to_string()).map(|typ| Self {
                            typ,
                            aggregator: None,
                        })
                    }
                    // #[metric(number, add)]
                    3 => {
                        let typ = Self::parse_type(&trees[0].to_string())?;
                        let aggregator = Self::parse_aggregator(&trees[2].to_string())?;
                        return Some(Self {
                            typ,
                            aggregator: Some(aggregator),
                        });
                    }
                    _ => return None,
                }
            }
        }

        None
    }
}

struct MetricField {
    metric_metadata: MetricMetadata,
    field_name: Ident,
}

impl MetricField {
    fn try_from_field(field: Field) -> Option<Self> {
        for attr in field.attrs.iter() {
            if !attr.path.is_ident("metric") {
                continue;
            }

            let field_name = field.ident.expect("Metric field must have a name");
            let metric_metadata =
                MetricMetadata::try_from_tokens(&attr.tokens).expect("Unknown metric type");
            return Some(Self {
                metric_metadata,
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
            let metadata = &metric_field.metric_metadata;
            let aggregator = &metadata.aggregator;
            let metric_type = &metadata.typ;
            let metric = if let Some(aggregator) = aggregator {
                quote! { ::trace_metric::Metric::#metric_type(stringify!(#field_name).to_string(),
                                                        self.#field_name,
                                                        Some(::trace_metric::metric::MetricAggregator::#aggregator))
                }
            } else {
                quote! { ::trace_metric::Metric::#metric_type(stringify!(#field_name).to_string(),
                                                        self.#field_name,
                                                        None)
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
