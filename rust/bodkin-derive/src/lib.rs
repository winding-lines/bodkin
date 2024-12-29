#![allow(unused_imports)]
extern crate proc_macro;

use std::any::Any;

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

    pub fn arrow_datatype(&self) -> proc_macro2::TokenStream {
        match self.inner_type.to_string().as_str() {
            "u8" if !self.array => quote_spanned! { self.span => arrow_array::UInt8Array},
            "u32" if !self.array => quote_spanned! { self.span => arrow_array::UInt32Array},
            "f32" if !self.array => quote_spanned! { self.span => arrow_array::Float32Array},
            "str" if !self.array => quote_spanned! { self.span => arrow_array::StringArray},
            _ if self.array => quote_spanned! { self.span => arrow_array::ListArray},
            _ => unimplemented!(),
        }
    }

    // In a Batch struct all the fields become arrays, compute a name for them.
    pub fn array_field_name(&self) -> Ident {
        Ident::new(&format!("{}s", self.name.to_string()), self.span)
    }
}

struct Schema {
    name: Ident,
    fields: Vec<BatchField>,
}

impl Schema {
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
        Ok(Schema { name, fields })
    }

    fn declare_batch_fields(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|entry| {
                let field_name = entry.array_field_name();
                let ty = entry.arrow_datatype();

                quote_spanned! {  entry.span => pub #field_name: #ty }
            })
            .collect::<Vec<_>>()
    }
}

struct Generator {
    schema: Schema,
}

impl Generator {
    fn name(&self) -> Ident {
        Ident::new(
            format!("{}Batch", self.schema.name.to_string()).as_str(),
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

    fn field_from_batch(batch_field: &BatchField) -> TokenStream2 {
        let name = batch_field.array_field_name();
        let column_name = batch_field.name.to_string();
        let err_missing = format!("missing column '{}'", column_name);
        let err_invalid = format!("invalid column '{}'", column_name);

        let ty = batch_field.arrow_datatype();
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

    fn batch_impl(&self) -> TokenStream2 {
        let builder_name = self.name();
        let fields = self
            .schema
            .fields
            .iter()
            .map(|field| Self::field_from_batch(field))
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
}

fn impl_my_trait(ast: DeriveInput) -> Result<TokenStream2> {
    let schema = Schema::new(ast)?;
    let generator = Generator { schema };
    let mut stream = TokenStream2::new();
    stream.extend(vec![generator.declare_batch_struct(), generator.batch_impl()].into_iter());
    Ok(stream)
}
