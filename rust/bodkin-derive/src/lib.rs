#![allow(unused_imports)]
extern crate proc_macro;

use std::{any::Any, panic::panic_any};

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, quote_spanned, ToTokens};
use syn::{
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
    spanned::Spanned,
    Result, *,
};

#[proc_macro_derive(ToRecordBatch)]
pub fn rule_system_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as _);
    TokenStream::from(match impl_my_trait(ast) {
        Ok(it) => it,
        Err(err) => err.to_compile_error(),
    })
}

struct BatchField {
    pub span: Span,
    pub name: Ident,
    inner_type: TokenStream2,
    nullable: bool,
    array: bool,
}

impl BatchField {
    fn new(field: Field) -> Self {
        let (inner_type, nullable, array) = match &field.ty {
            Type::Path(path) => {
                let last = path.path.segments.last().expect("has last");

                // if Vec<u8> then inner should be Vec<u8> and array is false

                let inner = match &last.arguments {
                    PathArguments::AngleBracketed(args) => args
                        .args
                        .first()
                        .expect("has one type argument")
                        .into_token_stream(),
                    _ => path.into_token_stream(),
                };

                let last_ident = last.ident.to_string();
                let is_vec = last_ident.as_str() == "Vec";
                let is_binary = is_vec && inner.to_string() == "u8";
                let nullable = last_ident.as_str() == "Option";

                let (inner, array) = if is_binary {
                    (last.into_token_stream(), false)
                } else {
                    (inner, is_vec)
                };

                (inner, nullable, array)
            }

            other => (other.into_token_stream(), false, false),
        };

        Self {
            span: field.span(),
            name: field.ident.expect("field is named"),
            inner_type,
            nullable,
            array,
        }
    }

    pub fn arrow_array_type(&self) -> proc_macro2::TokenStream {
        match self.inner_type.to_string().as_str() {
            "u8" if !self.array => quote_spanned! { self.span => arrow_array::UInt8Array},
            "u32" if !self.array => quote_spanned! { self.span => arrow_array::UInt32Array},
            "f32" if !self.array => quote_spanned! { self.span => arrow_array::Float32Array},
            "String" if !self.array => quote_spanned! { self.span => arrow_array::StringArray},
            _ if self.array => quote_spanned! { self.span => arrow_array::ListArray},
            _ => panic_any(format!("Unsupported type: {}", self.inner_type)),
        }
    }

    fn rust_type_to_arrow_data_type(&self, item_type: &str) -> proc_macro2::TokenStream {
        match item_type {
            "u8" => quote_spanned! { self.span => arrow::datatypes::DataType::UInt8},
            "u32" => quote_spanned! { self.span => arrow::datatypes::DataType::UInt32},
            "f32" => quote_spanned! { self.span => arrow::datatypes::DataType::Float32},
            "String" => quote_spanned! { self.span => arrow::datatypes::DataType::Utf8},
            _ => panic_any(format!("Unsupported type: {}", item_type)),
        }
    }

    pub fn arrow_data_type(&self) -> proc_macro2::TokenStream {
        let item_type = self.inner_type.to_string();
        if self.array {
            let sub_type = self.rust_type_to_arrow_data_type(item_type.as_str());
            quote_spanned! { self.span => arrow::datatypes::DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new("item", #sub_type, true)))}
        } else {
            self.rust_type_to_arrow_data_type(item_type.as_str())
        }
    }

    // In a Batch struct all the fields become arrays, compute a name for them.
    pub fn array_field_name(&self) -> Ident {
        Ident::new(&format!("{}s", self.name), self.span)
    }
}

// The schema of the input struct written by the user.
struct RowSchema {
    name: Ident,
    fields: Vec<BatchField>,
}

impl RowSchema {
    pub fn new(ast: DeriveInput) -> Result<Self> {
        let fields_named = match ast.data {
            Data::Enum(DataEnum {
                enum_token: token::Enum { span },
                ..
            })
            | Data::Union(DataUnion {
                union_token: token::Union { span },
                ..
            }) => {
                return Err(Error::new(span, "Expected a `struct`"));
            }

            Data::Struct(DataStruct {
                fields: Fields::Named(it),
                ..
            }) => it,

            Data::Struct(_) => {
                return Err(Error::new(
                    Span::call_site(),
                    "Expected a `struct` with named fields",
                ));
            }
        };

        let fields = fields_named
            .named
            .into_iter()
            .map(BatchField::new)
            .collect::<Vec<_>>();

        let name = ast.ident;
        Ok(RowSchema { name, fields })
    }

    fn declare_batch_fields(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|entry| {
                let field_name = entry.array_field_name();
                let ty = entry.arrow_array_type();

                quote_spanned! {  entry.span => pub #field_name: #ty }
            })
            .collect::<Vec<_>>()
    }
}

struct Generator {
    schema: RowSchema,
}

impl Generator {
    fn name(&self) -> Ident {
        Ident::new(
            format!("{}Batch", self.schema.name).as_str(),
            self.schema.name.span(),
        )
    }

    fn declare_batch_struct(&self) -> TokenStream2 {
        let gen_fields = self.schema.declare_batch_fields();
        let builder_name = self.name();
        quote! {
            pub struct #builder_name {
                #(#gen_fields ,)*
            }
        }
    }

    fn load_field_from_arrow(batch_field: &BatchField) -> TokenStream2 {
        let name = batch_field.array_field_name();
        let column_name = batch_field.name.to_string();
        let err_missing = format!("missing column '{}'", column_name);
        let err_invalid = format!("invalid column '{}'", column_name);

        let ty = batch_field.arrow_array_type();
        let stream = quote_spanned! { batch_field.span =>
         let #name = batch
            .column_by_name(#column_name)
            .ok_or_else(|| bodkin::BodkinError::new(#err_missing.into()))?
            .as_any()
            .downcast_ref::<#ty>()
            .ok_or_else(|| bodkin::BodkinError::new(#err_invalid.into()))?;
        };
        stream
    }

    fn impl_try_from_record_batch(&self) -> TokenStream2 {
        let builder_name = self.name();
        let fields = self
            .schema
            .fields
            .iter()
            .map(Self::load_field_from_arrow)
            .collect::<Vec<_>>();
        let field_names = self
            .schema
            .fields
            .iter()
            .map(|field| {
                let name = field.array_field_name();
                quote_spanned! { field.span=>#name : #name.clone()}
            })
            .collect::<Vec<_>>();
        quote! {
            impl #builder_name {

                pub fn try_from_record_batch(batch: &arrow_array::RecordBatch) -> std::result::Result<Self, bodkin::BodkinError> {
                    #(#fields ;)*

                    std::result::Result::Ok(#builder_name{ #(#field_names ,)*})
                }

            }
        }
    }

    fn impl_arrow_schema(&self) -> TokenStream2 {
        let builder_name = self.name();
        let fields = self
            .schema
            .fields
            .iter()
            .map(|field| {
                let name = field.name.to_string();
                let data_type = field.arrow_data_type();
                let nullable = field.nullable;
                quote_spanned! { field.span => arrow::datatypes::Field::new(#name, #data_type, #nullable)}
            })
            .collect::<Vec<_>>();
        quote! {
            impl #builder_name {
                pub fn arrow_schema() -> arrow::datatypes::Schema {
                    let fields = vec![#(#fields ,)*];
                    arrow::datatypes::Schema::new(fields)
                }
            }
        }
    }
}

fn impl_my_trait(ast: DeriveInput) -> Result<TokenStream2> {
    let schema = RowSchema::new(ast)?;
    let generator = Generator { schema };
    let mut stream = TokenStream2::new();
    stream.extend(
        vec![
            generator.declare_batch_struct(),
            generator.impl_try_from_record_batch(),
            generator.impl_arrow_schema(),
        ],
    );
    Ok(stream)
}
