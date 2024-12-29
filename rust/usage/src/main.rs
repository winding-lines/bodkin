use bodkin::ToRecordBatch;

#[derive(ToRecordBatch)]
pub struct Main {
    pub id: u32,
    pub image_id: u32,
    pub category_id: u32,
    pub bbox: Vec<f32>,
    pub area: f32,
    pub iscrowd: u8,
}

fn main() {
    println!("Hello, world!");
}
