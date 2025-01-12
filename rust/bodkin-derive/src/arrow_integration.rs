//! Implement the ArrowIntegration derive macro.

use std::panic::panic_any;

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, quote_spanned, ToTokens};
use syn::{spanned::Spanned, Result, *};

use darling::{FromDeriveInput, FromField};
#[derive(FromDeriveInput, FromField, Default, Debug)]
#[darling(default, attributes(arrow))]
pub(crate) struct Opts {
    pub datatype: Option<String>,
    pub metadata: Option<Vec<LitStr>>,
}

// Allowed values for the `datatype` attribute.
const DATA_TYPE_BINARY: &str = "Binary";
const DATA_TYPE_LARGE_BINARY: &str = "LargeBinary";

/// A field in the input struct written by the user.
///
/// This code is adapted from prost-arrow.
struct RowField {
    pub span: Span,
    pub name: Ident,
    inner_type: TokenStream2,
    nullable: bool,
    array: bool,
    requires_cloning: bool,
    // User field level options.
    user_opts: Opts,

}

impl RowField {
    fn new(field: Field) -> Self {
        let opts = Opts::from_field(&field).expect("has opts");
        let (inner_type, nullable, array, requires_cloning) = match &field.ty {
            Type::Path(path) => {
                let last = path.path.segments.last().expect("has last");

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
                let nullable = last_ident.as_str() == "Option";

                (inner, nullable, is_vec, last_ident.as_str() == "String")
            }

            other => (other.into_token_stream(), false, false, false),
        };

        Self {
            span: field.span(),
            name: field.ident.expect("field is named"),
            inner_type,
            nullable,
            array,
            requires_cloning,
            user_opts: opts,
        }
    }

    /// Generate the token stream for this RowField that will be used for primitive types of for the inner type of the Arrow array.
    ///
    /// This is used when instantiating an Arrow array.
    pub fn arrow_array_inner_type(&self) -> proc_macro2::TokenStream {
        if let Some(user_datatype) = &self.user_opts.datatype {
            match user_datatype.as_str() {
                DATA_TYPE_BINARY => {
                    return quote_spanned! { self.span => arrow_array::BinaryArray};
                }
                DATA_TYPE_LARGE_BINARY => {
                    return quote_spanned! { self.span => arrow_array::DATA_TYPE_LARGE_BINARYBinaryArray};
                }
                _ => panic_any(format!(
                    "Unsupported datatype in arrow_array_inner_type: {:?}",
                    user_datatype
                )),
            }
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

    fn is_user_binary(&self) -> bool {
        self.user_opts.datatype.as_deref() == Some(DATA_TYPE_BINARY)
    }

    fn is_user_large_binary(&self) -> bool {
        self.user_opts.datatype.as_deref() == Some(DATA_TYPE_LARGE_BINARY)
    }    

    /// Generate the token stream for the Arrow data type.
    ///
    /// This is used when defining the Arrow schema.
    pub fn arrow_data_type(&self) -> proc_macro2::TokenStream {
        if let Some(user_datatype) = &self.user_opts.datatype {
            match user_datatype.as_str() {
                DATA_TYPE_BINARY => {
                    return quote_spanned! { self.span => arrow::datatypes::DataType::Binary};
                }
                DATA_TYPE_LARGE_BINARY => {
                    return quote_spanned! { self.span => arrow::datatypes::DataType::LargeBinary};
                }
                _ => panic_any(format!(
                    "Unsupported datatype in arrow_data_type: {:?}",
                    user_datatype
                )),
            }
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

/// Code to generate an Arrow array from a vector of values.
struct ArrowArrayGenerator<'a> {
    row_field: &'a RowField,
    name: Ident,
    ty: TokenStream2,
}

impl<'a> ArrowArrayGenerator<'a> {
    pub fn new<'b: 'a>(row_field: &'b RowField) -> Self {
        let name = row_field.array_field_name().clone();

        let ty = row_field.arrow_array_inner_type();
        Self {
            row_field,
            name,
            ty,
        }
    }

    fn column_name(&self) -> &Ident {
        &self.row_field.name
    }

    /// Token stream for generating a list array from a vector of values.
    fn generate_list_array(&self) -> TokenStream2 {
        // The generated code works better inline a the top scope instead of in a block.
        // So generate identifier names for the values, offsets, and the list array.
        let values_name = Ident::new(
            format!("{}_values", self.name).as_str(),
            self.row_field.span,
        );
        let offsets_name = Ident::new(
            format!("{}_offsets", self.name).as_str(),
            self.row_field.span,
        );
        let offsets_arrow = Ident::new(
            format!("{}_offsets_arrow", self.name).as_str(),
            self.row_field.span,
        );
        let data_type = self.row_field.arrow_data_type();
        let data_name = Ident::new(format!("{}_data", self.name).as_str(), self.row_field.span);
        let ty = &self.ty;
        let column_name = self.column_name();
        let name = &self.name;
        quote_spanned! { self.row_field.span =>
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
    }

    fn generate_binary_array(&self) -> TokenStream2 {

            let column_name = self.column_name();
            let name = &self.name;
            quote_spanned! { self.row_field.span =>
                let #name = arrow_array::BinaryArray::from_iter_values(
                    items
                        .iter()
                        .map(|item| item.#column_name.as_slice())
                );
            }
    }
    
    fn generate_large_binary_array(&self) -> TokenStream2 {

            let column_name = self.column_name();
            let name = &self.name;
            quote_spanned! { self.row_field.span =>
                let #name = arrow_array::LargeBinaryArray::from_iter_values(
                    items
                        .iter()
                        .map(|item| item.#column_name.as_slice())
                );
            }
    }

    fn generate_cloned_array(&self) -> TokenStream2 {
        let column_name = self.column_name();
        let ty = &self.ty;

        let name = &self.name;
        quote_spanned! { self.row_field.span =>
            let #name = #ty::from(
                items
                    .iter()
                    .map(|item| item.#column_name.clone())
                    .collect::<Vec<_>>(),
            );
        }
    }

    fn generate_primitive_array(&self) -> TokenStream2 {
        let column_name = self.column_name();
        let ty = &self.ty;

        let name = &self.name;
        quote_spanned! { self.row_field.span =>
            let #name = #ty::from(
                items
                    .iter()
                    .map(|item| item.#column_name)
                    .collect::<Vec<_>>(),
            );
        }
    }

    /// Generate the token stream for generating a list of primitive values from a record batch, for a given field.
    fn rust_vec_to_arrow_array(self) -> (Ident, TokenStream2) {
        let arrow_array = if self.row_field.is_user_binary() {
            self.generate_binary_array()
        } else if self.row_field.is_user_large_binary() {
            self.generate_large_binary_array()
        } else if self.row_field.array {
            self.generate_list_array()
        } else if self.row_field.requires_cloning {
            self.generate_cloned_array()
        } else {
            self.generate_primitive_array()
        };
        (self.name.clone(), arrow_array)
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
                let mut simple_field = quote_spanned! { field.span => arrow::datatypes::Field::new(#name, #data_type, #nullable)};
                let metadata = field.user_opts.metadata.as_ref();
                if let Some(metadata) = metadata {
                        let key = &metadata[0];
                        let value = &metadata[1];
                        simple_field = quote_spanned! { field.span => #simple_field.with_metadata(std::iter::once((#key.to_string(), #value.to_string())).collect())};
                }
                simple_field
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

    /// Implement the `to_record_batch` method for the generated struct.
    fn impl_to_record_batch(self) -> TokenStream2 {
        let builder_name = self.name();
        let (names, arrow_arrays): (Vec<_>, Vec<_>) = self
            .schema
            .fields
            .iter()
            .map(|f| ArrowArrayGenerator::new(f).rust_vec_to_arrow_array())
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

pub(crate) fn impl_arrow_integration(ast: DeriveInput) -> Result<TokenStream2> {
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

    #[test]
    fn test_impl_arrow_schema() {
        let input: DeriveInput = parse_quote! {
            struct TestStruct {
                field1: i32,
                field2: Option<String>,
                field3: Vec<bool>
            }
        };

        let schema = RowSchema::new(input).unwrap();
        let generator = Generator { schema };

        let schema_impl = generator.impl_arrow_schema().to_string();

        // Check generated implementation contains expected field definitions
        assert!(schema_impl.contains("arrow :: datatypes :: Field :: new (\"field1\" , arrow :: datatypes :: DataType :: Int32 , false)"));
        assert!(schema_impl.contains("arrow :: datatypes :: Field :: new (\"field2\" , arrow :: datatypes :: DataType :: Utf8 , true)"));
        assert!(schema_impl.contains("arrow :: datatypes :: Field :: new (\"field3\" , arrow :: datatypes :: DataType :: List"));
    }

    #[test]
    fn test_try_from_record_batch() {
        let input: DeriveInput = parse_quote! {
            struct TestStruct {
                int_field: i32,
                string_field: String
            }
        };

        let schema = RowSchema::new(input).unwrap();
        let generator = Generator { schema };

        let impl_code = generator.impl_try_from_record_batch().to_string();

        // Check generated implementation contains expected field loading code
        assert!(impl_code.contains("column_by_name (\"int_field\")"));
        assert!(impl_code.contains("column_by_name (\"string_field\")"));
        assert!(impl_code.contains("downcast_ref :: < arrow_array :: Int32Array >"));
        assert!(impl_code.contains("downcast_ref :: < arrow_array :: StringArray >"));
    }
}
