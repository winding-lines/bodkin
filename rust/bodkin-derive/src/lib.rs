//! This is an internal library for the `bodkin` crate.
//! See <https://docs.rs/bodkin> for more information.

use proc_macro::TokenStream;
use syn::parse_macro_input;
extern crate proc_macro;



mod arrow_integration;

#[proc_macro_derive(ArrowIntegration, attributes(arrow))]
pub fn rule_system_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as _);
    TokenStream::from(match arrow_integration::impl_arrow_integration(ast) {
        Ok(it) => it,
        Err(err) => err.to_compile_error(),
    })
}
