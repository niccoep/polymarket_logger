use std::time::Instant;
use backtest::parallel::process_files;
use std::path::PathBuf;
use glob::glob;

use backtest::Result;

fn main() -> Result<()> {
    //println!("Rayon threads: {}", rayon::current_num_threads());
    //rayon::ThreadPoolBuilder::new()
    //    .num_threads(8)
    //    .build_global()
    //    .unwrap();

    let start = Instant::now();
    let mut glob_path = PathBuf::from("./market_data");
    glob_path.push("btc/*");
    glob_path.push("*/*.parquet");
    let pattern = glob_path.to_str().unwrap();

    let file_glob: Vec<PathBuf> = glob(pattern)?
        .map(|path| path.expect("error on glob path"))
        .collect();

    let results = process_files(file_glob);
    println!("results:\n {:?}", results);
    println!("duration: {:?}", start.elapsed());

    Ok(())
}
