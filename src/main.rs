use std::collections::HashMap;
use std::collections::HashSet;

use std::fs::File;
use std::path::PathBuf;
use std::io::{BufReader, BufRead, Cursor, Write, Read};
use std::time::Instant;
use anyhow::{anyhow, Result, Error};
use clap::{Parser, Subcommand};
use serde_json;
use serde_json::Value;
use serde::{Deserialize, Serialize};
use flate2::read::MultiGzDecoder;   
use flate2::write::GzEncoder;
use flate2::Compression;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

use indicatif::{ProgressBar,ProgressStyle};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;
use threadpool::ThreadPool;
use glob::glob; 
use std::hash::{Hash, Hasher, DefaultHasher};
use dashmap::DashMap;
use rand::thread_rng;
use rand::Rng;

use rand::seq::SliceRandom;
use rayon::prelude::*;
use itertools::Itertools;

use crate::io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, has_json_extension};

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
    },



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

fn build_exact_profile(config: &PathBuf, output: &PathBuf) -> Result<(), Error> {
    // Data structure here we want to save is just a vector of groups
    // where each group is a vector of (path_id, line_num) tuples

    let config_contents = read_pathbuf_to_mem(config).unwrap().into_inner().into_inner();
    let config = DupConfig::from_json_bytes(&config_contents).unwrap();
    let grouper : DashMap::<u64, Vec<(usize, usize)>> = DashMap::new();
    let pbar = build_pbar(config.indices.len(), "Paths");
    config.indices.par_iter()
        .for_each(|(p, idx)| {
            collect_exact_dups(p, *idx, &grouper).unwrap()
        });

    let groups: Vec<Vec<(usize, usize)>> = grouper
        .iter()
        .par_bridge()
        .map(|e| e.value().clone())
        .collect();

    let json_groups = serde_json::to_string(&groups).unwrap().into_bytes();
    write_mem_to_pathbuf(&json_groups, output).unwrap();

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
        Commands::ExactProfile {config, output} => {
            build_exact_profile(&config, output)
        }
    };


}



