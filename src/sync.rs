use std::io;
use std::time::SystemTime;

use std::fs::File;
use std::io::BufReader;

use arrow::record_batch::RecordBatch;

use chrono::NaiveDateTime;

use zip::CompressionMethod;
use zip::ZipArchive;
use zip::read::ZipFile;

use crate::core::Method;
use crate::core::Zip;
use crate::core::ZipFileMeta;
use crate::core::ZipItemMeta;

pub fn n2sys_utc(n: NaiveDateTime) -> SystemTime {
    n.and_utc().into()
}

pub fn zfile2imeta<R>(zfile: &ZipFile<R>) -> ZipItemMeta
where
    R: io::Read,
{
    let name: String = zfile.name().to_string();
    let comment: String = zfile.comment().to_string();

    let method: Method = match zfile.compression() {
        CompressionMethod::Stored => Method::Store,
        CompressionMethod::Deflated => Method::Deflate,
        _ => Method::Store,
    };

    let zip_dt: Option<_> = zfile.last_modified();
    let naive: Option<NaiveDateTime> = zip_dt.and_then(|d| d.try_into().ok());
    let stime: Option<SystemTime> = naive.map(n2sys_utc);

    let modified: SystemTime = stime.unwrap_or(SystemTime::UNIX_EPOCH);

    let crc32: u32 = zfile.crc32();
    let compressed_size: u64 = zfile.compressed_size();
    let uncompressed_size: u64 = zfile.size();

    ZipItemMeta {
        name,
        comment,
        method,
        modified,
        crc32,
        compressed_size,
        uncompressed_size,
    }
}

pub fn comment2str(comment: &[u8]) -> &str {
    str::from_utf8(comment).unwrap_or_default()
}

pub fn zip2meta<R>(zip_id: String, mut z: ZipArchive<R>) -> Result<Zip, io::Error>
where
    R: io::Read + io::Seek,
{
    let comment: String = comment2str(z.comment()).into();

    let mut files: Vec<ZipItemMeta> = Vec::with_capacity(z.len());

    for i in 0..z.len() {
        let zfile = z.by_index(i)?;
        files.push(zfile2imeta(&zfile));
    }

    Ok(Zip {
        zip_id,
        meta: ZipFileMeta { comment, files },
    })
}

pub fn zip2record_batch<R>(zip_id: String, z: ZipArchive<R>) -> Result<RecordBatch, io::Error>
where
    R: io::Read + io::Seek,
{
    let zip: Zip = zip2meta(zip_id, z)?;
    crate::core::zip2record_batch(zip)
}

pub fn zipfile2record_batch(zip_id_as_filename: String) -> Result<RecordBatch, io::Error> {
    let file: File = File::open(&zip_id_as_filename)?;
    let reader: BufReader<_> = BufReader::new(file);
    let archive: ZipArchive<_> = ZipArchive::new(reader)?;

    zip2record_batch(zip_id_as_filename, archive)
}
