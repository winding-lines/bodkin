//! This is an internal library for the `bodkin` crate.
//! See <https://docs.rs/bodkin> for more information.

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

#[proc_macro_derive(ArrowIntegration)]
pub fn rule_system_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as _);
    TokenStream::from(match impl_my_trait(ast) {
        Ok(it) => it,
        Err(err) => err.to_compile_error(),
    })
}

/// A field in the input struct written by the user.
///
/// This code is adapted from prost-arrow.
struct RowField {
    pub span: Span,
    pub name: Ident,
    inner_type: TokenStream2,
    nullable: bool,
    array: bool,
    binary: bool,
}

impl RowField {
    fn new(field: Field) -> Self {
        let (inner_type, nullable, array, binary) = match &field.ty {
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
                let _is_binary = is_vec && inner.to_string() == "u8";
                let nullable = last_ident.as_str() == "Option";

                // Disable the binary detection for now, many to make this more configurable.
                let is_binary = false;

                let (inner, array) = if is_binary {
                    (last.into_token_stream(), false)
                } else {
                    (inner, is_vec)
                };

                (inner, nullable, array, is_binary)
            }

            other => (other.into_token_stream(), false, false, false),
        };

        Self {
            span: field.span(),
            name: field.ident.expect("field is named"),
            inner_type,
            nullable,
            array,
            binary,
        }
    }

    /// Generate the token stream for this RowField that will be used for primitive types of for the inner type of the Arrow array.
    /// 
    /// This is used when instantiating an Arrow array.
    pub fn arrow_array_inner_type(&self) -> proc_macro2::TokenStream {
        if self.binary {
            return quote_spanned! { self.span => arrow_array::BinaryArray};
        }
        match self.inner_type.to_string().as_str() {
            "String" => quote_spanned! { self.span => arrow_array::StringArray},
            "bool" => quote_spanned! { self.span => arrow_array::BooleanArray},
            "f32" => quote_spanned! { self.span => arrow_array::Float32Array},
            "f64" => quote_spanned! { self.span => arrow_array::Float64Array},
            "i16" => quote_spanned! { self.span => arrow_array::Int16Array},
            "i32" => quote_spanned! { self.span => arrow_array::Int32Array},
            "i64" => quote_spanned! { self.span => arrow_array::Int64Array},
            "i8" => quote_spanned! { self.span => arrow_array::Int8Array},
            "u16" => quote_spanned! { self.span => arrow_array::UInt16Array},
            "u32" => quote_spanned! { self.span => arrow_array::UInt32Array},
            "u64" => quote_spanned! { self.span => arrow_array::UInt64Array},
            "u8" => quote_spanned! { self.span => arrow_array::UInt8Array},
            _ => panic_any(format!(
                "Unsupported type in arrow_array_inner_type: {}",
                self.inner_type
            )),
        }
    }

    /// Generate the arrow array type for this RowField.
    ///
    /// This is used when instantiating an Arrow array.
    pub fn arrow_array_type(&self) -> proc_macro2::TokenStream {
        if !self.array {
            self.arrow_array_inner_type()
        } else {
            quote_spanned! { self.span => arrow_array::ListArray}
        }
    }

    /// Generate the token stream for the inner type of the Arrow data type.
    /// 
    /// This is used when defining the Arrow schema.
    fn arrow_data_inner_type(&self) -> proc_macro2::TokenStream {
        let item_type = self.inner_type.to_string();
        match item_type.as_str() {
            "String" => quote_spanned! { self.span => arrow::datatypes::DataType::Utf8},
            "bool" => quote_spanned! { self.span => arrow::datatypes::DataType::Bool},
            "f32" => quote_spanned! { self.span => arrow::datatypes::DataType::Float32},
            "f64" => quote_spanned! { self.span => arrow::datatypes::DataType::Float64},
            "i16" => quote_spanned! { self.span => arrow::datatypes::DataType::Int16},
            "i32" => quote_spanned! { self.span => arrow::datatypes::DataType::Int32},
            "i64" => quote_spanned! { self.span => arrow::datatypes::DataType::Int64},
            "i8" => quote_spanned! { self.span => arrow::datatypes::DataType::Int8},
            "u16" => quote_spanned! { self.span => arrow::datatypes::DataType::UIn16},
            "u32" => quote_spanned! { self.span => arrow::datatypes::DataType::UInt32},
            "u64" => quote_spanned! { self.span => arrow::datatypes::DataType::UInt64},
            "u8" => quote_spanned! { self.span => arrow::datatypes::DataType::UInt8},
            _ => panic_any(format!(
                "Unsupported type in arrow_data_inner_type: {:?}",
                item_type
            )),
        }
    }

    /// Generate the token stream for the Arrow data type.
    /// 
    /// This is used when defining the Arrow schema.
    pub fn arrow_data_type(&self) -> proc_macro2::TokenStream {
        if self.binary {
            // This respects the LanceDB convention.
            return quote_spanned! { self.span => arrow::datatypes::DataType::Binary}
        } 
        let inner = self.arrow_data_inner_type();
        if self.array {
            quote_spanned! { self.span => arrow::datatypes::DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new("item", #inner, true)))}
        } else {
            inner
        }
    }

    // In generated <UserType>Arrow struct all the fields become arrays, compute a name for them.
    pub fn array_field_name(&self) -> Ident {
        Ident::new(&format!("{}s", self.name), self.span)
    }
}

// The schema of the input struct written by the user.
struct RowSchema {
    name: Ident,
    fields: Vec<RowField>,
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
            .map(RowField::new)
            .collect::<Vec<_>>();

        let name = ast.ident;
        Ok(RowSchema { name, fields })
    }
}

/// Generator
///
/// Hold methods for generating the <User>Arrow struct and the <User>Arrow implementation.
struct Generator {
    schema: RowSchema,
}

impl Generator {
    /// The prefix for the generated struct and associated implementation.
    const IDENTIFIER_PREFIX: &'static str = "Arrow";

    /// The identifier of the generated struct and associated implementation.
    fn name(&self) -> Ident {
        Ident::new(
            format!("{}{}", self.schema.name, Self::IDENTIFIER_PREFIX).as_str(),
            self.schema.name.span(),
        )
    }

    /// Generate the token stream for the fields of the generated struct.
    fn declare_derived_arrow_fields(&self) -> Vec<proc_macro2::TokenStream> {
        self.schema
            .fields
            .iter()
            .map(|entry| {
                let field_name = entry.array_field_name();
                let ty = entry.arrow_array_type();

                quote_spanned! {  entry.span => pub #field_name: #ty }
            })
            .collect::<Vec<_>>()
    }

    /// Declare the derived <UserType>Arrow struct.
    fn declare_derived_arrow_struct(&self) -> TokenStream2 {
        let gen_fields = self.declare_derived_arrow_fields();
        let builder_name = self.name();
        quote! {
            pub struct #builder_name {
                #(#gen_fields ,)*
            }
        }
    }

    /// Generate the token stream for loading a an arrow column from a record batch.
    ///
    /// Note: the identifier `batch` is expected to be in scope and points to the record batch to load from.
    fn load_field_from_arrow(batch_field: &RowField) -> TokenStream2 {
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

    /// Implement the `TryFrom<RecordBatch>` trait for the generated struct.
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

                pub fn try_from_record_batch(batch: &arrow_array::RecordBatch) -> bodkin::Result<Self> {
                    #(#fields ;)*

                    bodkin::Result::Ok(#builder_name{ #(#field_names ,)*})
                }

            }
        }
    }

    /// Generate the token stream for the Arrow schema of the generated struct.
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

    /// Generate the token stream for generating a list of primitive values from a record batch, for a given field.
    fn rust_vec_to_arrow_array(row_field: &RowField) -> (Ident, TokenStream2) {
        let column_name = &row_field.name;
        let name = row_field.array_field_name().clone();

        let ty = row_field.arrow_array_inner_type();

        let arrow_array = if row_field.array || row_field.binary {
            // The generated code works better inline a the top scope instead of in a block.
            // So generate identifier names for the values, offsets, and the list array.
            let values_name = Ident::new(format!("{}_values", name).as_str(), row_field.span);
            let offsets_name = Ident::new(format!("{}_offsets", name).as_str(), row_field.span);
            let offsets_arrow =
                Ident::new(format!("{}_offsets_arrow", name).as_str(), row_field.span);
            let data_type = row_field.arrow_data_type();
            let data_name = Ident::new(format!("{}_data", name).as_str(), row_field.span);
            quote_spanned! { row_field.span =>
                let #values_name = #ty::from(
                    items
                        .iter()
                        .flat_map(|item| item.#column_name.clone())
                        .collect::<Vec<_>>(),
                );
                let mut #offsets_name: Vec<i32> = Vec::with_capacity(#values_name.len() + 1);
                #offsets_name.push(0);
                for (i, len) in items.iter().map(|item| item.#column_name.len() as i32).enumerate() {
                    #offsets_name.push(#offsets_name[i] + len);
                }
                let #offsets_arrow = arrow::array::Int32Array::from_iter(#offsets_name);
                let #data_name = arrow::array::ArrayDataBuilder::new(#data_type)
                    .len(#offsets_arrow.len() - 1)
                    .add_buffer(#offsets_arrow.into_data().buffers()[0].clone())
                    .add_child_data(#values_name.into_data())
                    .build()?;
            
                let #name = arrow::array::GenericListArray::<i32>::from(#data_name);
            }
        } else {
            quote_spanned! { row_field.span =>
                let #name = #ty::from(items
                    .iter()
                    .map(|item| item.#column_name.clone())
                    .collect::<Vec<_>>());

            }
        };
        (name, arrow_array)
    }

    /// Implement the `to_record_batch` method for the generated struct.
    fn impl_to_record_batch(self) -> TokenStream2 {
        let builder_name = self.name();
        let (names, arrow_arrays): (Vec<_>, Vec<_>) = self
            .schema
            .fields
            .iter()
            .map(Self::rust_vec_to_arrow_array)
            .unzip();
        let struct_row_name = self.schema.name;
        quote! {
            impl #builder_name {
                /// Convert a slice of `#struct_row_name` to an arrow `RecordBatch`, in a fallible way.
                pub fn to_record_batch(items: &[#struct_row_name]) -> bodkin::Result<arrow_array::RecordBatch> {
                    use arrow_array::Array;
                    let schema = Self::arrow_schema();

                    #(#arrow_arrays)*

                    let out = arrow_array::RecordBatch::try_new(std::sync::Arc::new(schema), vec![
                        #(std::sync::Arc::new(#names) ,)*
                    ])?;

                    bodkin::Result::Ok(out)
                }
            }
        }
    }
}

fn impl_my_trait(ast: DeriveInput) -> Result<TokenStream2> {
    let schema = RowSchema::new(ast)?;
    let generator = Generator { schema };
    let mut stream = TokenStream2::new();
    stream.extend(vec![
        generator.declare_derived_arrow_struct(),
        generator.impl_try_from_record_batch(),
        generator.impl_arrow_schema(),
        generator.impl_to_record_batch(),
    ]);
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use std::array;

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(quote!(Vec<u8>), false, true, "u8")]
    #[case(quote!(i8), false, false, "i8")]
    #[case(quote!(bool), false, false, "bool")]
    #[case(quote!(Option<String>), true, false, "String")]
    #[case(quote!(Vec<String>), false, true, "String")]
    fn test_row_field_new(
        #[case] ty: TokenStream2,
        #[case] nullable: bool,
        #[case] array: bool,
        #[case] inner_type: &str,
    ) {
        let field: Field = parse_quote! {
            pub data: #ty
        };
        let row_field = RowField::new(field);
        assert_eq!(row_field.name.to_string(), "data");
        assert_eq!(row_field.nullable, nullable);
        assert_eq!(row_field.array, array);
        assert_eq!(row_field.inner_type.to_string(), inner_type);
    }

    #[rstest]
    #[case(quote!(Vec<u8>),"arrow_array :: UInt8Array")]
    #[case(quote!(i8), "arrow_array :: Int8Array")]
    #[case(quote!(bool), "arrow_array :: BooleanArray")]
    #[case(quote!(Option<String>), "arrow_array :: StringArray")]
    #[case(quote!(Vec<String>), "arrow_array :: StringArray")]
    fn test_arrow_array_inner_type(#[case] ty: TokenStream2, #[case] expected: &str) {
        let field: Field = parse_quote! {
            pub small_int: #ty
        };
        let row_field = RowField::new(field);
        let generated = row_field.arrow_array_inner_type().to_string();
        assert_eq!(generated, expected);
    }

    #[rstest]
    #[case(quote!(f32), "arrow_array :: Float32Array")]
    #[case(quote!(Vec<u8>), "arrow_array :: ListArray")]
    #[case(quote!(Vec<i32>), "arrow_array :: ListArray")]
    fn test_arrow_array_type(#[case] ty: TokenStream2, #[case] expected: &str) {
        let field: Field = parse_quote! {
            pub my_numbers: #ty
        };
        let row_field = RowField::new(field);
        let array_type = row_field.arrow_array_type().to_string();
        assert_eq!(array_type, expected);
    }

    #[rstest]
    #[case(quote!(f64),"arrow :: datatypes :: DataType :: Float64")]
    #[case(quote!(i8), "arrow :: datatypes :: DataType :: Int8")]
    #[case(quote!(bool), "arrow :: datatypes :: DataType :: Bool")]
    fn test_arrow_data_inner_type(#[case] ty: TokenStream2, #[case] expected: &str) {
        let field: Field = parse_quote! {
            pub small_int2: #ty
        };
        let row_field = RowField::new(field);
        let generated = row_field.arrow_data_inner_type().to_string();
        assert_eq!(generated, expected);
    }

    #[rstest]
    #[case(quote!(Vec<i32>), "arrow :: datatypes :: DataType :: List (std :: sync :: Arc :: new (arrow :: datatypes :: Field :: new (\"item\" , arrow :: datatypes :: DataType :: Int32 , true)))")]
    #[case(quote!(Vec<u8>), "arrow :: datatypes :: DataType :: List (std :: sync :: Arc :: new (arrow :: datatypes :: Field :: new (\"item\" , arrow :: datatypes :: DataType :: UInt8 , true)))")]
    fn test_arrow_data_type(#[case] ty: TokenStream2, #[case] expected: &str) {
        let field: Field = parse_quote! {
            pub my_numbers: #ty
        };
        let row_field = RowField::new(field);
        let data_type = row_field.arrow_data_type().to_string();
        assert_eq!(data_type, expected);
    }
}
