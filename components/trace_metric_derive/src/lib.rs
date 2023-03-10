// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod builder;

use builder::Builder;

#[proc_macro_derive(TracedMetrics, attributes(metric))]
pub fn derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    Builder::parse_from_ast(ast).build()
}
