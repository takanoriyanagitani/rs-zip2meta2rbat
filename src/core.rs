use core::time::Duration;
use std::io;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::record_batch::RecordBatch;

use arrow::array::{
    ArrayRef, DictionaryArray, Int8Array, StringArray, TimestampMicrosecondArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum Method {
    Store = 0,
    Deflate = 8,
}

pub struct ZipItemMeta {
    pub name: String,
    pub comment: String,
    pub method: Method,
    pub modified: SystemTime,
    pub crc32: u32,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

pub struct ZipFileMeta {
    pub comment: String,
    pub files: Vec<ZipItemMeta>,
}

pub struct Zip {
    pub zip_id: String,
    pub meta: ZipFileMeta,
}

/*
pub struct FlatZipMetaInfo {
    pub zip_id: String,
    pub item_name: String,
    pub item_comment: String,
    pub item_modified: SystemTime,
    pub crc32: u32,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

impl FlatZipMetaInfo {
    pub fn item_modified_unixtime_us(&self) -> Option<u64> {
        stime2unixtime_us(self.item_modified)
    }
}
*/

pub fn stime2unixtime(s: SystemTime) -> Option<Duration> {
    s.duration_since(SystemTime::UNIX_EPOCH).ok()
}

pub fn duration2us(d: Duration) -> Option<u64> {
    let micros = d.as_micros();
    micros.try_into().ok()
}

pub fn stime2unixtime_us(s: SystemTime) -> Option<u64> {
    stime2unixtime(s).and_then(duration2us)
}

pub fn zip2record_batch(z: Zip) -> Result<RecordBatch, io::Error> {
    let num_rows: usize = z.meta.files.len();

    let single_zip_id = StringArray::from(vec![z.zip_id.clone()]);
    let zip_id_indices = Int8Array::from(vec![0; num_rows]);
    let zip_id_array: DictionaryArray<_> =
        DictionaryArray::try_new(zip_id_indices, Arc::new(single_zip_id))
            .map_err(io::Error::other)?;

    let mut item_names: Vec<String> = Vec::with_capacity(num_rows);
    let mut item_comments: Vec<String> = Vec::with_capacity(num_rows);
    let mut item_modified: Vec<Option<i64>> = Vec::with_capacity(num_rows);
    let mut crc32s: Vec<u32> = Vec::with_capacity(num_rows);
    let mut compressed_sizes: Vec<u64> = Vec::with_capacity(num_rows);
    let mut uncompressed_sizes: Vec<u64> = Vec::with_capacity(num_rows);

    let mut methods: Vec<u8> = Vec::with_capacity(num_rows);

    for f in &z.meta.files {
        item_names.push(f.name.clone());
        item_comments.push(f.comment.clone());

        let us_opt: Option<i64> = stime2unixtime_us(f.modified).map(|u| u as i64);
        item_modified.push(us_opt);

        crc32s.push(f.crc32);
        compressed_sizes.push(f.compressed_size);
        uncompressed_sizes.push(f.uncompressed_size);

        let m: Method = f.method;
        let mu: u8 = m as u8;
        methods.push(mu);
    }

    let schema = Schema::new(vec![
        Field::new(
            "zip_id",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("item_name", DataType::Utf8, false),
        Field::new("item_comment", DataType::Utf8, false),
        Field::new("item_method", DataType::UInt8, false),
        Field::new(
            "item_modified",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("item_crc32", DataType::UInt32, false),
        Field::new("item_compressed_size", DataType::UInt64, false),
        Field::new("item_uncompressed_size", DataType::UInt64, false),
    ]);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(zip_id_array),
        Arc::new(StringArray::from(item_names)),
        Arc::new(StringArray::from(item_comments)),
        Arc::new(UInt8Array::from(methods)),
        Arc::new(TimestampMicrosecondArray::from(item_modified)),
        Arc::new(UInt32Array::from(crc32s)),
        Arc::new(UInt64Array::from(compressed_sizes)),
        Arc::new(UInt64Array::from(uncompressed_sizes)),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays).map_err(io::Error::other)
}
