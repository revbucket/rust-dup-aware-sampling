use dashmap::{DashSet, DashMap};
use std::collections::HashMap;
use std::path::PathBuf;
use std::io::{BufRead};
use std::time::Instant;
use anyhow::{Result, Error};
use clap::{Parser, Subcommand};
use serde_json;
use serde_json::Value;
use serde::{Deserialize, Serialize};
use indicatif::{ProgressBar,ProgressStyle};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::hash::{Hash, Hasher, DefaultHasher};
use rayon::prelude::*;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

use crate::io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, has_json_extension};
use bincode;

pub mod s3;
pub mod io; 


/*
Multiple commands:
1. build-config : builds a way to map path -> path_id (assumes pool remain constant)
2. exact-profile : collects the exact-duplicate profile and saves it somewher
3. more TBD
*/



/*============================================
=            Args                            =
============================================*/

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ArgParser {
    #[clap(subcommand)]
    command: Commands,

    #[arg(long, default_value_t=0)]
    threads: usize,
}


#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true)]

    BuildConfig {
        // Just makes and saves the path lookup object 

        /// Input locations for paths to hash
        #[arg(required=true, long, num_args=1..)]
        input: Vec<PathBuf>,        

        /// Output location (may be an s3 uri)
        #[arg(required=true, long)]
        output: PathBuf,
    },


    ExactProfile {
        // Takes the config and builds an "exact duplicate" profile
        #[arg(required=true, long)]
        config: PathBuf,   

        #[arg(required=true, long)]
        output: PathBuf,

        #[arg(long)]
        save_ids_only: bool,
    },


    TrueDupSeries {
        #[arg(required=true, long)]
        group_ids: PathBuf,

        #[arg(required=true, long)]
        polling_freq: usize,

        #[arg(required=true, long)]
        output: PathBuf,
    },

    BuildGoodToulminProfile {
        #[arg(required=true, long)]
        group_ids: PathBuf,

        #[arg(required=true, long, num_args=1..)]
        sample_freq: Vec<usize>,

        #[arg(required=true, long)] 
        output: PathBuf,
    }



}




/*================================================
=            Utilities/Helpers                   =
================================================*/

fn build_pbar(num_items: usize, units: &str) -> ProgressBar {
    let mut template = String::from(units);
    template.push_str(" {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]");
    let pbar = ProgressBar::new(num_items as u64)
        .with_style(
            ProgressStyle::with_template(&template).unwrap()
        );
    pbar.inc(0);
    pbar
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



/*=================================================
=                  Config builder                 =
=================================================*/
#[derive(Serialize, Deserialize)]
pub struct DupConfig {
    pub input: Vec<PathBuf>,
    pub indices: HashMap<PathBuf, usize>
}

impl DupConfig {
    pub fn new(input: &Vec<PathBuf>) -> Result<Self, Error> {
        let mut paths = expand_dirs(input.clone(), None).unwrap();
        paths.sort();
        let indices: HashMap::<PathBuf, usize> = paths.iter()
            .enumerate()
            .map(|(i,p)| (p.clone(), i))
            .collect();
        Ok(DupConfig {input: input.clone(), indices})

    }
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        let json_string = serde_json::to_string(self).unwrap();
        Ok(json_string.into_bytes())
    }
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let json_string = String::from_utf8(bytes.to_vec()).unwrap();
        let config: DupConfig = serde_json::from_str(&json_string).unwrap();
        Ok(config)
    }
}

fn build_config(input: &Vec<PathBuf>, output: &PathBuf) -> Result<DupConfig, Error> {

    let config = DupConfig::new(input).unwrap();
    let json_bytes = config.to_json_bytes().unwrap();

    let output = if has_json_extension(&output) {
        output.clone()
    } else {
        output.clone().join("config.json.gz")
    };
    write_mem_to_pathbuf(&json_bytes, &output).unwrap();

    Ok(config)
}


/*=================================================
=              Exact Duplicate Profile            =
=================================================*/

fn build_exact_profile(config: &PathBuf, output: &PathBuf, save_ids_only: bool) -> Result<(), Error> {
    // Data structure here we want to save is just a vector of groups
    // where each group is a vector of (path_id, line_num) tuples

    let config_contents = read_pathbuf_to_mem(config).unwrap().into_inner().into_inner();
    let config = DupConfig::from_json_bytes(&config_contents).unwrap();
    let grouper : DashMap::<u64, Vec<(usize, usize)>> = DashMap::new();
    let pbar = build_pbar(config.indices.len(), "Paths");
    config.indices.par_iter()
        .for_each(|(p, idx)| {
            let result = collect_exact_dups(p, *idx, &grouper).unwrap();
            pbar.inc(1);
            result
        });

    let groups: Vec<Vec<(usize, usize)>> = grouper
        .iter()
        .par_bridge()
        .map(|e| e.value().clone())
        .collect();


    if save_ids_only {
        // Flat list of group_ids saving some preprocessing time:
        // i.e., groups [[doc1, doc2], [doc3], [doc4, doc5, doc6]] -> [0, 0, 1, 2, 2, 2]
        let group_ids = AtomicUsize::new(0);
        let group_pbar = build_pbar(groups.len(), "Groups");
        let flat_groups: Vec<usize> = groups.par_iter()
            .flat_map(|g| {
                let group_id = group_ids.fetch_add(1, Ordering::SeqCst);
                let new_vec: Vec<usize> = g.iter().map(|_| group_id).collect();
                group_pbar.inc(1);
                new_vec
            }).collect();

        let encoded: Vec<u8> = bincode::serialize(&flat_groups).unwrap();
        write_mem_to_pathbuf(&encoded, output).unwrap();


    } else {
        // Portable (json) list of all groups, where each group is a Vec<(usize, usize)>
        let json_groups = serde_json::to_string(&groups).unwrap().into_bytes();
        write_mem_to_pathbuf(&json_groups, output).unwrap();
    }

    Ok(())
}


fn collect_exact_dups(path: &PathBuf, path_idx: usize, grouper: &DashMap<u64, Vec<(usize, usize)>>) -> Result<(), Error> {
    let contents = read_pathbuf_to_mem(path).unwrap();
    let mut line_count: usize = 0;
    for line in contents.lines() {
        let doc_id = (path_idx, line_count);
        let line = line.unwrap();
        let json: Value = serde_json::from_str(&line).unwrap();
        let text = json["text"].as_str().unwrap();
        let text_hash = hash_str(text, 0);
        grouper.entry(text_hash).or_default().push(doc_id);
        line_count += 1;
    }
    Ok(())
}


/*=======================================================
=                    True Dup Series                    =
=======================================================*/

fn true_dup_series(group_ids: &PathBuf, polling_freq: usize, output: &PathBuf) -> Result<(), Error> {
    let start_main = Instant::now();

    println!("Reading group contents into memory...");
    let start_read = Instant::now();
    let group_contents = read_pathbuf_to_mem(group_ids).unwrap();

    let ext = group_ids.extension().and_then(|s| s.to_str()).unwrap();
    let group_contents: Vec<usize> = if ext == "json" {
        serde_json::from_slice(&group_contents.into_inner().into_inner()).unwrap()
    } else {
        bincode::deserialize(&group_contents.into_inner().into_inner()).unwrap()
    };
    println!("Read group contents in {:?} secs", start_read.elapsed().as_secs());

    println!("Starting shuffle...");
    let start_shuffle = Instant::now();
    let group_contents = parallel_shuffle(group_contents);
    println!("Shuffle completed in {:?} secs", start_shuffle.elapsed().as_secs());

    // Not super threadsafe here, but that's okay
    println!("Starting poll...");
    let start_poll = Instant::now();
    let uniques : DashSet<usize> = DashSet::new();
    let total_seen = AtomicUsize::new(0);
    let reports: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let pbar = build_pbar(group_contents.len(), "Docs");
    group_contents.into_par_iter()
        .for_each(|id| {
            let count = total_seen.fetch_add(1, Ordering::SeqCst);
            uniques.insert(id);
            if count / polling_freq > reports.lock().unwrap().len() {
                let mut locked_reports = reports.lock().unwrap();
                if count / polling_freq > locked_reports.len() {
                    locked_reports.push((count, uniques.len()));
                }
            }
            pbar.inc(1);
        });
    println!("Added {:?} elements to poll in {:?} secs", reports.lock().unwrap().len(), start_poll.elapsed().as_secs());


    // And then save these somewhere
    let json_bytes = serde_json::to_vec(&reports.lock().unwrap().clone()).unwrap();
    write_mem_to_pathbuf(&json_bytes, &output).unwrap();

    println!("-----------------");
    println!("Finishing true_dup_series in {:?} secs", start_main.elapsed().as_secs());
    Ok(())
}


fn parallel_shuffle<T: Send>(v: Vec<T>) -> Vec<T> {
    let len = v.len();
    let v = Mutex::new(v);
    let pbar = build_pbar(len, "Items");
    (0..len).into_par_iter().for_each(|i| {
        let mut rng = thread_rng();
        let j = rng.gen_range(i..len);
        if i != j {
            let mut v = v.lock().unwrap();
            v.swap(i, j);
        }
        pbar.inc(1);
    });

    v.into_inner().unwrap()
}


/*===========================================================
=                    Good-Toulmin Profiles                  =
===========================================================*/
#[derive(Serialize, Deserialize)]
struct GTSeries {
    size: usize,
    freq: HashMap<usize, usize>
}


fn build_good_toulmin_profile(group_ids: &PathBuf, sample_freq: &Vec<usize>, output: &PathBuf) -> Result<(), Error> {
    let start_main = Instant::now();

    println!("Reading group contents into memory...");
    let start_read = Instant::now();
    let group_contents = read_pathbuf_to_mem(group_ids).unwrap();

    let ext = group_ids.extension().and_then(|s| s.to_str()).unwrap();
    let group_contents: Vec<usize> = if ext == "json" {
        serde_json::from_slice(&group_contents.into_inner().into_inner()).unwrap()
    } else {
        bincode::deserialize(&group_contents.into_inner().into_inner()).unwrap()
    };


    println!("Read group contents in {:?} secs", start_read.elapsed().as_secs());

    println!("Starting shuffle...");
    let start_shuffle = Instant::now();
    let mut group_contents = parallel_shuffle(group_contents);
    println!("Shuffle completed in {:?} secs", start_shuffle.elapsed().as_secs());


    println!("Starting GT Build...");
    let start_gt = Instant::now();
    let total_seen = AtomicUsize::new(0);
    let pbar = build_pbar(group_contents.len(), "Docs");
    let counter: DashMap<usize, usize> = DashMap::new();
    let mut sorted_sample_freqs = sample_freq.clone();
    sorted_sample_freqs.sort();
    sorted_sample_freqs.reverse();
    let max_freq = sorted_sample_freqs[0];
    group_contents.truncate(max_freq);
    let mut heldout_sample_freqs = sorted_sample_freqs.clone();
    heldout_sample_freqs.reverse();
    let sorted_sample_freqs = Arc::new(Mutex::new(sorted_sample_freqs));
    let freq_maps: Arc<Mutex<Vec<HashMap<usize, usize>>>> = Arc::new(Mutex::new(Vec::new()));

    // Plan here is to:
    // Iterate in parallel over shuffled list
    // For each element:
    //  - increment total_seen
    //  - increment id in counter 
    //  - if seen enough longer than the min sorted_sample_freqs, 
    //      + lock and clone the counter (process into frequency maps)
    //      + pop the last element from the sorted_sampl_freqs

    group_contents.into_par_iter()
        .for_each(|id| {
            let count = total_seen.fetch_add(1, Ordering::SeqCst);
            counter.entry(id).or_insert(0);
            counter.alter(&id, |_, count| count +1);
            let last = *sorted_sample_freqs.lock().unwrap().last().unwrap();
            if last <= count {
                let mut locked_freqs = sorted_sample_freqs.lock().unwrap();
                let checkpoint = if *locked_freqs.last().unwrap() <= count+1 {
                    locked_freqs.pop().unwrap()
                } else {
                    0
                };
                if checkpoint > 0 {
                    let counter_clone = counter.clone();
                    freq_maps.lock().unwrap().push(make_freq_map(counter_clone));
                }
            }
            pbar.inc(1);
        }); 
    println!("Build GTs in {:?} secs", start_gt.elapsed().as_secs());

    // Now make the thing to write
    let to_save: Vec<GTSeries> = heldout_sample_freqs.into_iter()
        .zip(freq_maps.lock().unwrap().iter())
        .map(|(size, freq)| GTSeries { size, freq: freq.clone() })
        .collect();
    let json_bytes = serde_json::to_vec(&to_save).unwrap();
    write_mem_to_pathbuf(&json_bytes, &output).unwrap();


    println!("-----------------");
    println!("Finishing GT estimator buliding in {:?} secs", start_main.elapsed().as_secs());
    Ok(())
}

fn make_freq_map(counter: DashMap<usize, usize>) -> HashMap<usize, usize> {
    let mut freq_map: HashMap<usize, usize> = HashMap::new();
    counter.iter().for_each(|entry| {
        let value = entry.value();
        *freq_map.entry(*value).or_insert(0) += 1;
    });

    freq_map
}


/*=================================================
=                Main logic flow                  =
=================================================*/


fn main() {
    let args = ArgParser::parse();
    let threads = args.threads;
    if threads != 0 {
        std::env::set_var("RAYON_NUM_THREADS", threads.to_string());
    }

    let result = match &args.command {
        Commands::BuildConfig {input, output} => {
            let result = build_config(input, output);
            result.unwrap();
            Ok(())
        },
        Commands::ExactProfile {config, output, save_ids_only} => {
            build_exact_profile(&config, output, *save_ids_only)
        },
        Commands::TrueDupSeries {group_ids, polling_freq, output} => {
            true_dup_series(group_ids, *polling_freq, output)
        },
        Commands::BuildGoodToulminProfile {group_ids, sample_freq, output} => {
            build_good_toulmin_profile(group_ids, sample_freq, output)
        }
    };

    result.unwrap();

}



