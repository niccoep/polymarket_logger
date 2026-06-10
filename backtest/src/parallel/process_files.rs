use std::time::Instant;

use rayon::prelude::*;
use std::path::PathBuf;

use crate::reconstruction::BookReplay;
use crate::stats::StatsAccumulator;
use crate::traits::BookProcessor;
use crate::Result;

pub fn process_files(file_paths: Vec<PathBuf>) -> Vec<Result<usize>> {
    //unsafe { std::env::set_var("POLARS_MAX_THREADS", "1") };

    file_paths
        .par_chunks(10)
        .flat_map_iter(|batch|
            batch
            .iter()
            .map(|path| -> Result<usize> {
                let mut stats = StatsAccumulator::new();
                println!("Start: {:?} at {:?}", path.file_name(), Instant::now());

                BookReplay::from_parquet(path)?
                    .replay(|timestamp, snapshot| {
                        stats.process(timestamp, snapshot);
                    });
                println!("End: {:?}", path.file_name());
                Ok(stats.finalize())
            })
            .collect::<Vec<_>>()
        )
        .collect()
}
