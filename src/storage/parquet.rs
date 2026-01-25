
use arrow::array::{ArrayRef, UInt8Array, Float32Array, Int64Array, Int16Array, RecordBatch};
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;

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
    row_buffer: Vec<BookLevel>,
    batch_size: usize,
    file_path: PathBuf,
    rows_written: u64,
}

impl ParquetWriter {
    pub fn new(file_path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = create_schema();

        //let file = File::create(&file_path).expect(format!("Failed to create file: {:?}", file_path).as_str());
        let file = File::create(&file_path)?;

        let writer_properties = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(4)?))
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_properties))?;

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

        self.writer.write(&batch)?;

        self.rows_written += num_rows as u64;
        self.row_buffer.clear();

        println!("Flushed {} rows to {:?} (total: {})",
            num_rows,
            self.file_path,
            self.rows_written
        );

        Ok(())
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
        let asset_binary_array = Arc::new(UInt8Array::from(asset_binaries)) as ArrayRef;
        let side_array = Arc::new(UInt8Array::from(sides)) as ArrayRef;
        let price_bps_array = Arc::new(Int16Array::from(price_bpss)) as ArrayRef;
        let size_array = Arc::new(Float32Array::from(sizes)) as ArrayRef;

        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![timestamp_array, 
                asset_binary_array, 
                side_array,
                price_bps_array,
                size_array]
        )?)
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        
        self.writer.close()
            .expect("Failed to close ParquetWriter");

        println!(
            "Closed parquet file {:?} with {} total rows", 
            self.file_path,
            self.rows_written
        );

        Ok(())
    }
}
