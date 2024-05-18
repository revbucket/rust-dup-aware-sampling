use std::collections::HashMap;
use std::collections::HashSet;

use std::fs::File;
use std::path::PathBuf;
use std::io::{BufReader, BufRead, Cursor, Write, Read};
use std::time::Instant;
use anyhow::{anyhow, Result, Error};
use clap::{Parser};
use serde_json;
use serde_json::Value;
use flate2::read::MultiGzDecoder;   
use flate2::write::GzEncoder;
use flate2::Compression;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::s3::{is_s3, expand_s3_dir, get_reader_from_s3, write_cursor_to_s3};
use indicatif::{ProgressBar,ProgressStyle};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;
use threadpool::ThreadPool;
use glob::glob; 
use std::hash::{Hash, Hasher, DefaultHasher};
use dashmap::DashMap;
use rand::thread_rng;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use itertools::Itertools;

pub mod s3;

/*============================================
=            Args                            =
============================================*/

#[derive(Parser, Debug)]
struct Args {
    /// (List of) directories/files (on s3 or local) that are jsonl.gz or jsonl.zstd files
    #[arg(required=true, long)]
    input: Vec<PathBuf>,

    /// Output location (may be an s3 uri)
    #[arg(required=true, long)]
    output: PathBuf,

    /// How many copies are allowed of a single document
    #[arg(required=true, long)]
    subsample_rate: f64,

    /// How many threads to use (default is max available)
    #[arg(long, default_value_t=0)]
    threads: usize,

    /// Seed to initialize hasher 
    #[arg(long, default_value_t=1234)]
    hash_seed: usize,

    /// How many times to retry s3 operations
    #[arg(long, default_value_t=3)]
    s3_retries: usize,

    /// Should we save duplicate profiles? 
    /// (if so, save them in the output directory as input_profile.json, output_profile.json)
    #[arg(long, default_value_t=false)]
    save_profiles: bool,


    /// If this is true, save profiles only! (and don't make a new dataset) 
    #[arg(long, default_value_t=false)]
    profile_only: bool,
}


/*================================================
=            Utilities/Helpers                   =
================================================*/

async fn expand_dirs(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {    
    // Can handle both local and s3 files/directories
    // Note that this function is async because we need to wait for all the s3 files to get expanded
    // Also note that the output vector is SORTED (in case S3 has inconsistent list_objects_v2 ordering)
    let mut files: Vec<PathBuf> = Vec::new();
    for path in paths {

        if is_s3(path) {
            let s3_result = expand_s3_dir(path).await?;
            for file in s3_result {
                files.push(file.clone());
            }
        } else if path.is_dir() {
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("invalid path '{}'", path.to_string_lossy()))?;
            for entry in glob(&format!("{}/**/*.json*.gz", path_str))? {
                files.push(entry?.to_path_buf());
            }
        } else {
            files.push(path.clone());
        }
    }   
    files.sort();
    Ok(files)
}


fn get_output_filename(inputs: &[PathBuf], input_filename: &PathBuf, output_directory: &PathBuf) -> PathBuf {
    // More fancy output-file naming that no longer assumes unique inputs
    let matching_prefix = inputs
        .iter()
        .find(|pfx| input_filename.starts_with(pfx))
        .expect("No matching prefix found?!?");

    let relative_path = input_filename.strip_prefix(matching_prefix).unwrap();
    output_directory.clone().join(relative_path)

}

fn read_file_into_memory(input_file: &PathBuf) ->Result<Cursor<Vec<u8>>, Error>{
    // Takes a local file (must be local!) and reads it into a Cursor of bytes
    let mut file = File::open(input_file).expect("Failed to open file");

    let mut contents = Vec::new();
    let ext = input_file.extension().unwrap().to_string_lossy().to_lowercase();
    if ext == "gz" {
        // Gzip case        
        let mut decoder = MultiGzDecoder::new(file);
        decoder.read_to_end(&mut contents).expect("Failed to read loca gzip file");
    } else if ext == "zstd" || ext == "zst" {
        // Zstd case
        let mut decoder = ZstdDecoder::new(file).unwrap();
        decoder.read_to_end(&mut contents).expect("Failed to read local zstd file");
    } else {
        file.read_to_end(&mut contents).expect("Failed to read local file");

        // No compression case 
    }
    Ok(Cursor::new(contents))
}


fn hash_str(text: &str, seed: usize) -> u64 {
    // Hashes a vector of type T into a u64 hash value
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    text.hash(&mut hasher);
    hasher.finish()
}


fn reverse_map(map: &DashMap<u64, usize>) -> HashMap<usize, Vec<u64>> {
    let grouped: HashMap<usize, Vec<u64>> = map
        .iter().par_bridge()
        .fold(|| HashMap::new(), |mut acc: HashMap<usize, Vec<u64>>, ref_multi| {
            acc.entry(*ref_multi.value()).or_default().push(*ref_multi.key());
            acc
        })
        .reduce(|| HashMap::new(), |mut acc, map| {
            for (key, value) in map {
                acc.entry(key).or_default().extend(value);
            }
            acc
        });

    grouped
}

/*=========================================================
=                Process file fxn + helpers               =
=========================================================*/

async fn count_doc_freqs(input: &PathBuf, counter: &Arc<DashMap<u64, usize>>,
                         hash_seed: usize, s3_retries: usize, pbar: Arc<Mutex<ProgressBar>>) -> Result<(), Error> {
    // Loops through all document 'text's in a jsonl, hashes the document and 
    // increments the hash counter in the dup map

    // Step a: Load file into memory/lines
    let reader = if is_s3(input) {
        get_reader_from_s3(input, Some(s3_retries)).await.unwrap()
    } else {
        BufReader::new(read_file_into_memory(&input).unwrap())
    };

    // Step b: Iterate over lines and count frequencies
    for line in reader.lines() {
        let line = line?;
        let json: Value = serde_json::from_str(&line)?;
        let text = json["text"].as_str().unwrap();
        let hash_val = hash_str(&text, hash_seed);
        counter.entry(hash_val).or_insert(0);
        counter.alter(&hash_val, |_, count| count +1);
    }
    pbar.lock().unwrap().inc(1);
    Ok(())
}


async fn prune_from_hashset(input: &PathBuf, output: &PathBuf, keep_hashes: &Arc<HashSet<u64>>,
                      hash_seed: usize, s3_retries: usize, total_docs: Arc<AtomicUsize>, removed_docs: Arc<AtomicUsize>,
                      pbar: Arc<Mutex<ProgressBar>>) -> Result<(), Error> {

    // Step a: Load file into memory/lines
    let reader = if is_s3(input) {
        get_reader_from_s3(input, Some(s3_retries)).await.unwrap()
    } else {
        BufReader::new(read_file_into_memory(&input).unwrap())
    };


    // Step b: Iterate over lines and check if seen so many duplicates
    let mut cur_lines = 0;
    let mut cur_removed = 0;
    let mut output_lines : Vec<String> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let json: Value = serde_json::from_str(&line)?;
        let text = json["text"].as_str().unwrap();  
        let hash_val = hash_str(&text, hash_seed);
        cur_lines += 1;
        if keep_hashes.contains(&hash_val) {
            output_lines.push(line);
        } else {
            cur_removed += 1;
        }
    }

    let output_lines = output_lines.join("\n");
    total_docs.fetch_add(cur_lines, Ordering::SeqCst);
    removed_docs.fetch_add(cur_removed, Ordering::SeqCst);

    // Step c: Take lines and write to output file, modify loggers
    if output_lines.len() == 0 {
        return Ok(());
    }
    let output_bytes = output_lines.as_bytes();
    let output_bytes = match output.extension().unwrap().to_str() {
        Some("gz") => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&output_bytes).unwrap();            
            encoder.finish().unwrap()
        },
        Some("zstd") => {
            let mut encoder = ZstdEncoder::new(Vec::new(), 0).unwrap();
            encoder.write_all(&output_bytes).unwrap();
            encoder.finish().unwrap()
        },
        _ => {output_bytes.to_vec()}
    };

    if is_s3(output) {
        let cursor = Cursor::new(output_bytes.to_vec());
        write_cursor_to_s3(&output, cursor).await.expect(format!("Unable to write to output file {:?}", output).as_str());
    } else {
        let mut file = File::create(output).expect(format!("Unable to create output file {:?}", output).as_str());
        file.write_all(&output_bytes).expect(format!("Unable to write to {:?}", output).as_str());
    }
    pbar.lock().unwrap().inc(1);
    Ok(())
}


fn get_dup_profile(rev_map: &HashMap<usize, Vec<u64>>) -> HashMap<usize, f64> {

    let profile: HashMap<usize, usize>= rev_map
        .par_iter()
        .map(|(k,v)| (*k, *k as usize * v.len() as usize))
        .collect();

    let total_items = profile.par_iter().map(|(_,v)| v).sum::<usize>() as f64;

    let output: HashMap<usize, f64> = profile.par_iter().map(|(k, v)| (*k, *v as f64 / total_items)).collect();
    output
    
}


async fn save_profile(profile: HashMap<usize, f64>, dst: PathBuf) -> Result<(), Error> {
    let profile_bytes = serde_json::to_vec(&profile).unwrap();

    if is_s3(dst.clone()) {
        let cursor = Cursor::new(profile_bytes);
        write_cursor_to_s3(&dst, cursor).await.unwrap();
    } else {
        let mut file = File::create(dst).unwrap();
        file.write_all(&profile_bytes.as_slice()).unwrap();
    }

    Ok(())
}



/*=================================================
=                Main logic flow                  =
=================================================*/


#[tokio::main]
async fn main()-> Result<()> {

    // Step 1: initialize things and collect files 
    let start_time = Instant::now();

    let args = Args::parse();
    let threads = if args.threads == 0 {
        available_parallelism().unwrap().get()
    } else {
        args.threads
    };
    let input_files = expand_dirs(&args.input).await?;

    println!("INPUTS {:?}", input_files);
    let pbar = ProgressBar::new(input_files.len() as u64)
        .with_style(
            ProgressStyle::with_template(
                "Files {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]",
            ).unwrap()
        );
    let pbar = Arc::new(Mutex::new(pbar));
    let dup_map = Arc::new(DashMap::new());


    // Step 2: Iterate over all files and count frequencies
    let threadpool = ThreadPool::new(threads);
    for input in &input_files { 
        let input = input.clone();   
        let pbar = pbar.clone();
        let dup_map = Arc::clone(&dup_map);
        threadpool.execute(move || {        
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();   
            let result = rt.block_on({
                count_doc_freqs(
                    &input,
                    &dup_map,
                    args.hash_seed,
                    args.s3_retries,
                    pbar
                    )
                });            
            match result {            
                Err(err) => {
                    eprintln!("Error processing {:?}; {:?}", input, err);
                },
                _ => {},
            };
        });
    }
    threadpool.join();

    // Step 3: Create set of hashes to keep
    let mut rev_map = reverse_map(&dup_map);
    let input_profile = get_dup_profile(&rev_map);
    rev_map.par_iter_mut().for_each(|(_, values)| {
        let mut rng = rand::thread_rng();
        let target_size = (values.len() as f64 * args.subsample_rate).round() as usize;
        values.shuffle(&mut rng);
        values.truncate(target_size);
    });
    let output_profile = get_dup_profile(&rev_map);
    let keep_hashes: HashSet<u64> = rev_map.par_iter()
        .flat_map(|(_, values)| values.par_iter().cloned())
        .collect();

    if args.save_profiles {
        save_profile(input_profile, args.output.join("input_profile.json")).await.unwrap();
        save_profile(output_profile, args.output.join("output_profile.json")).await.unwrap();
    }
    if args.profile_only {
        return Ok(());
    }

    println!("TOTAL HASHES {:?} | KEPT HAHSES {:?}",
             dup_map.len(), keep_hashes.len());
    let keep_hashes = Arc::new(keep_hashes);


    // Step 4: Prune and only keep docs that hash to the kept values
    println!("Actually pruning dataset!");
    let pbar = ProgressBar::new(input_files.len() as u64)
        .with_style(
            ProgressStyle::with_template(
                "Files {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]",
            ).unwrap()
        );
    let pbar = Arc::new(Mutex::new(pbar));
    // Step 2: Iterate over all files and count frequencies
    let threadpool = ThreadPool::new(threads);
    let total_docs = Arc::new(AtomicUsize::new(0));
    let removed_docs = Arc::new(AtomicUsize::new(0));    
    for input in &input_files {    
        let input = input.clone();
        let output = get_output_filename(&args.input, &input, &args.output);        
        let pbar = pbar.clone();
        let keep_hashes = keep_hashes.clone();        
        let total_docs = total_docs.clone();
        let removed_docs = removed_docs.clone();

        threadpool.execute(move || {        
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();   
            let result = rt.block_on({
                prune_from_hashset(
                    &input,
                    &output,
                    &keep_hashes,
                    args.hash_seed,
                    args.s3_retries,
                    total_docs, 
                    removed_docs,
                    pbar
                    )
                });            
            match result {            
                Err(err) => {
                    eprintln!("Error processing {:?}; {:?}", input, err);
                },
                _ => {},
            };
        });
    }
    threadpool.join();    



    println!("Finishing exact dedup run!");
    println!("-------------------------------");
    println!("Ran in {:?} (s)", start_time.elapsed().as_secs());
    println!("Saw {:?} documents | Removed {:?} of them", 
             total_docs.fetch_add(0, Ordering::SeqCst),
             removed_docs.fetch_add(0, Ordering::SeqCst));




    Ok(())
}



