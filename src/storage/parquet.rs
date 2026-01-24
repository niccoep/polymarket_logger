
use arrow::array::{RecordBatch};
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::models::events::BookLevel;

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("asset_id", DataType::UInt8, false),
        Field::new("side", DataType::UInt8, false),
        Field::new("price_bps", DataType::Int16, false),
        Field::new("size", DataType::Float32, false),
    ]))
}

pub struct ParquetWriter {
    schema: SchemaRef,
    writer: ArrowWriter<File>,
    row_buffer: Vec<BookLevel>, //TODO
    batch_size: usize,
    file_path: PathBuf,
    rows_written: u64,
}
