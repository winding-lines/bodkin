use std::{error::Error, fmt, sync::Arc};

#[allow(unused_imports)]
#[macro_use]
extern crate bodkin_derive;

use arrow::array::ArrayDataBuilder;
use arrow_array::{Array, ArrowNumericType, GenericListArray, OffsetSizeTrait, PrimitiveArray};
use arrow_schema::{DataType, Field};
#[doc(hidden)]
pub use bodkin_derive::*;

#[derive(Debug)]
pub enum BodkinError {
    ArrowError(arrow::error::ArrowError),
    MacroError(String),
}

impl BodkinError {
    pub fn new(message: String) -> Self {
        BodkinError::MacroError(message)
    }
}

impl Error for BodkinError {}

impl fmt::Display for BodkinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BodkinError::ArrowError(err) => return write!(f, "ArrowError: {}", err),
            BodkinError::MacroError(message) => return write!(f, "MacroError: {}", message),
            
        }
    }
}

impl From<arrow::error::ArrowError> for BodkinError {
    fn from(err: arrow::error::ArrowError) -> Self {
        BodkinError::ArrowError(err)
    }
}

/// Create an [`GenericListArray`] from values and offsets.
///
/// ```
/// use arrow_array::{Int32Array, Int64Array, ListArray};
/// use arrow_array::types::Int64Type;
/// use lance_arrow::try_new_generic_list_array;
///
/// let offsets = Int32Array::from_iter([0, 2, 7, 10]);
/// let int_values = Int64Array::from_iter(0..10);
/// let list_arr = try_new_generic_list_array(int_values, &offsets).unwrap();
/// assert_eq!(list_arr,
///     ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
///         Some(vec![Some(0), Some(1)]),
///         Some(vec![Some(2), Some(3), Some(4), Some(5), Some(6)]),
///         Some(vec![Some(7), Some(8), Some(9)]),
/// ]))
/// ```
pub fn try_new_generic_list_array<T: Array, Offset: ArrowNumericType>(
    values: T,
    offsets: &PrimitiveArray<Offset>,
) -> Result<GenericListArray<Offset::Native>, arrow::error::ArrowError>
where
    Offset::Native: OffsetSizeTrait,
{
    let data_type = if Offset::Native::IS_LARGE {
        DataType::LargeList(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            true,
        )))
    } else {
        DataType::List(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            true,
        )))
    };
    let data = ArrayDataBuilder::new(data_type)
        .len(offsets.len() - 1)
        .add_buffer(offsets.into_data().buffers()[0].clone())
        .add_child_data(values.into_data())
        .build()?;

    Ok(GenericListArray::from(data))
}