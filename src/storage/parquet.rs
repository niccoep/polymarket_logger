
use arrow::array::{ArrayRef, Float32Array, Int64Array, RecordBatch};
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

impl ParquetWriter {
    pub fn new(file_path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = create_schema();

        let file = File::create(&file_path).expect(format!("Failed to create file: {:?}", file_path).as_str());

        let writer_properties = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(4)?))
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_properties))
            .expect(format!("Failed ot create ArrowWriter with properties: {:?}", writer_properties).to_str());

        println!("Created parquet writer for file: {:?}", file_path);

        Ok(Self {
            schema,
            writer,
            row_buffer: Vec::with_capacity(batch_size),
            batch_size,
            file_path,
            rows_written: 0,
        })
    }

    pub fn add_row(&mut self, data: BookLevel) -> Result<()> {
        self.row_buffer.push(data);

        if self.row_buffer.len() >= self.batch_size {
            self.flush()?
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.row_buffer.is_empty() {
            return Ok(());
        }

        let batch = self.buffer_to_record_batch()?;
        let num_rows = batch.num_rows();

        self.writer.write(&batch)
            .expect("failed to write RecordBatch");

        self.rows_written += num_rows as u64;
        self.row_buffer.clear();

        println!("Flushed {} rows to {:?} (total: {})",
            num_rows,
            self.file_path,
            self.rows_written
        );

        Ok(());
    }

    fn buffer_to_record_batch(&self) -> Result<RecordBatch> {
        let len = self.row_buffer.len();

        let mut timestamps = Vec::with_capacity(len);
        let mut asset_binaries = Vec::with_capacity(len);
        let mut sides = Vec::with_capacity(len);
        let mut price_bpss = Vec::with_capacity(len);
        let mut sizes = Vec::with_capacity(len);

        for row in &self.row_buffer {
            timestamps.push(row.timestamp);
            asset_binaries.push(row.asset_binary);
            sides.push(row.side);
            price_bpss.push(row.price_bps);
            sizes.push(row.size);
        }

        let timestamp_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
    }
}
