use std::slice;

use arrow::array::AsArray;
use arrow::datatypes::Float32Type;
use bodkin::ArrowIntegration;


#[derive(Debug, PartialEq, ArrowIntegration)]
pub struct Main {
    pub id: u32,
    pub image_file: String,
    pub category_id: u32,
    pub bbox: Vec<f32>,
    pub area: f32,
    pub iscrowd: u8,
    pub count: i16,
}

#[derive(Debug, PartialEq, ArrowIntegration)]
pub struct Blobish {
    #[arrow(datatype = "Binary")]
    pub binary: Vec<u8>,
}

fn generate_some_data() -> (Main, Blobish) {
    (
        Main {
            id: 1,
            image_file: "image1.jpg".to_string(),
            category_id: 1,
            bbox: vec![0.0, 1.0, 2.0, 3.0],
            area: 1.0,
            iscrowd: 0,
            count: 1,
        },
        Blobish {
            binary: vec![0, 1, 2, 3],
        },
    )
}

fn main() {
    println!("Generated schema: {:#?}", MainArrow::arrow_schema());
    let (data, blobish) = generate_some_data();
    let record_batch = MainArrow::to_record_batch(slice::from_ref(&data))
        .expect("Failed to convert to record batch");
    println!("Generated record batch: {:#?}", record_batch);
    let round_trip_data =
        MainArrow::try_from_record_batch(&record_batch).expect("Failed to read from record batch");
    assert_eq!(data.id, round_trip_data.ids.value(0));
    assert_eq!(data.image_file, round_trip_data.image_files.value(0));

    // The first sub-array in `bboxs` should be the same as the `bbox` field in the original data.
    assert_eq!(4, round_trip_data.bboxs.value_length(0));

    // Get the first sub-array.
    let first = round_trip_data.bboxs.value(0);
    // Cast it to a primitive array of f32.
    let first_f32 = first.as_primitive::<Float32Type>();
    // Since the items are optional in arrow, they will be wrapped in Some() when accessed.
    let first_as_vec = first_f32.iter().flatten().collect::<Vec<_>>();

    assert_eq!(data.bbox, first_as_vec);
}
