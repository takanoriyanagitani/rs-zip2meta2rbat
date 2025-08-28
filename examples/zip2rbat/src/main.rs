use std::env;
use std::io;
use std::process::ExitCode;

use rs_zip2meta2rbat::arrow;

use arrow::record_batch::RecordBatch;

use rs_zip2meta2rbat::sync::zipfile2record_batch;

fn print_record_batch(b: &RecordBatch) -> Result<(), io::Error> {
    println!("{b:#?}");
    Ok(())
}

fn env2zip_filename() -> Result<String, io::Error> {
    let zip_filename = env::var("ZIP_FILENAME").map_err(io::Error::other)?;
    Ok(zip_filename)
}

fn sub() -> Result<(), io::Error> {
    let zfilename: String = env2zip_filename()?;
    let rb: RecordBatch = zipfile2record_batch(zfilename)?;
    print_record_batch(&rb)?;
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
