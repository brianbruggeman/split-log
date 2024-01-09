use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};

use clap::Parser;
use chrono::NaiveDateTime;
use serde_json::Value;
use flate2::write::GzEncoder;
use flate2::Compression;
use num_format::{SystemLocale, ToFormattedString};

#[derive(Parser)]
struct Opts {
    #[clap(short, long)]
    input: String,
    #[clap(short, long, default_value="")]
    output: String,
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.output.as_str() {
        "-" => {
            let reader = build_reader(&opts.input)?;
            let mut lines = reader.lines();
            while let Some(Ok(line)) = lines.next() {
                println!("{}", line);
            }
        }
        "" => {
            let output_path = opts.input.replace(".json.1", "");
            process_log_file(&opts.input, &output_path)?;
        }
        _ => {
            process_log_file(&opts.input, &opts.output)?;
        }
    }
    process_log_file(&opts.input, &opts.output)?;
    Ok(())
}

/// Creates a buffered reader for the given path.
fn build_reader(path: &str) -> anyhow::Result<BufReader<std::fs::File>> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(e) => {
            anyhow::bail!("{e}.  Could not open path for reading: `{path}`");
        }
    };
    let reader = BufReader::new(file);
    Ok(reader)
}

/// Creates the parent directory for the given path.
fn create_parent(path: &str) -> anyhow::Result<()> {
    let parent_path = std::path::Path::new(path).parent().expect("Could not get parent path");
    if let Err(why) = std::fs::create_dir_all(&parent_path) {
        anyhow::bail!("{why}.  Could not create directory: `{}`", parent_path.display());
    }
    Ok(())
}

/// Dumps the line into a gzipped jsonl log
fn dump_line(file: &mut BufWriter<File>, path: &str, line: &str) -> anyhow::Result<()> {
    let mut gz = GzEncoder::new(file, Compression::default());

    if let Err(why) = gz.write_all(line.as_bytes()) {
        anyhow::bail!("{why}.  Could not write line `{line}` to file: `{path}`");
    }
    if let Err(why) = gz.write_all(b"\n") {
        anyhow::bail!("{why}.  Could not write newline to file: `{path}`");
    }
    if let Err(why) = gz.finish() {
        anyhow::bail!("{why}.  Could not finish writing to file: `{path}`");
    }
    Ok(())
}

/// Opens the given path for appending.  Creates the file if it does not exist.
fn open_append_file(path: &str) -> anyhow::Result<std::fs::File> {
    let file = match OpenOptions::new().append(true).create(true).open(path) {
        Ok(file) => file,
        Err(e) => {
            anyhow::bail!("{e}.  Could not open path for writing: `{path}`");
        }
    };
    Ok(file)
}

/// Parses a log line for a timestamp from the `asctime` field.
fn parse_date(line: &str) -> anyhow::Result<NaiveDateTime> {
    let log_entry = parse_line(line)?;
    if !log_entry.is_object() {
        anyhow::bail!("Line is not a JSON object: `{line}`");
    }
    match log_entry["asctime"].as_str() {
        Some(asctime) => {
            match NaiveDateTime::parse_from_str(asctime, "%Y-%m-%d %H:%M:%S,%f") {
                Ok(timestamp) => Ok(timestamp),
                Err(e) => {
                    anyhow::bail!("{e}.  Could not parse timestamp: `{asctime}`");
                }
            }
        }
        None => {
            anyhow::bail!("No `asctime` field found in line: {line}")
        }
    }
}

/// Converts a line into a JSON object.
fn parse_line(line: &str) -> anyhow::Result<Value> {
    let log_entry: Value = match serde_json::from_str(&line) {
        Ok(log_entry) => log_entry,
        Err(e) => {
            anyhow::bail!("{e}.  Could not parse line: `{line}`");
        }
    };
    Ok(log_entry)
}

/// Processes a full log file and shards it into daily, gzipped log files.
fn process_log_file(input: &str, output_path: &str) -> anyhow::Result<()> {
    // Hold the handlers open for better performance
    let system_locale = SystemLocale::default()?;
    let mut file_handlers = HashMap::new();
    let error_handler_filepath = format!("{}.error.gz", output_path);
    create_parent(&error_handler_filepath).expect("Could not create parent directory");
    let file = open_append_file(&error_handler_filepath).expect("Could not open file for writing");
    let mut error_handler = BufWriter::new(file);
    let start = std::time::Instant::now();
    let mut log_start = std::time::Instant::now();
    let reader = match build_reader(input) {
        Ok(reader) => reader,
        Err(e) => {
            anyhow::bail!("{e}.  Could not open path for reading: `{input}`");
        }
    };
    let mut lines = reader.lines();
    let mut line_count = 0;
    let mut entry_count = 0;
    let mut last_line_date = None;
    while let Some(Ok(line)) = lines.next() {
        let line_date = match parse_date(&line) {
            Ok(timestamp) => Some(timestamp.date()),
            Err(e) => {
                eprintln!("Error {e}. Error processing line {line_count}: `{line}`");
                dump_line(&mut error_handler, &error_handler_filepath, &line)?;
                continue
            }
        };
        if last_line_date.is_some() && line_date != last_line_date {
            let log_elapsed = log_start.elapsed();
            let pretty_human_duration = humantime::format_duration(log_elapsed);
            log_start = std::time::Instant::now();
            println!("Completed processing {}.  {} records. [Took: {pretty_human_duration}]", last_line_date.unwrap(), entry_count.to_formatted_string(&system_locale));
            entry_count = 0;
            file_handlers.remove_entry(&last_line_date.unwrap());
        }
        let filename = format!("{output_path}.{}.jsonl.gz", line_date.unwrap().format("%Y-%m-%d"));
        let mut file_handler = file_handlers.entry(line_date.unwrap()).or_insert_with(|| {
            create_parent(&filename).expect("Could not create parent directory");
            let file = open_append_file(&filename).expect("Could not open file for writing");
            BufWriter::new(file)
        });
        if let Err(why) = dump_line(&mut file_handler, &filename, &line) {
            eprintln!("Error {why}. Error processing line {line_count}: `{line}`");
            dump_line(&mut error_handler, &error_handler_filepath, &line)?;
        }
        last_line_date = line_date;
        line_count += 1;
        entry_count += 1;
    }
    let duration = start.elapsed();
    let pretty_human_duration = humantime::format_duration(duration);
    println!("Finished processing {line_count} lines in {pretty_human_duration}.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date() {
        let line = r#"{"asctime": "2021-03-01 00:00:00,000", "message": "test"}"#;
        let timestamp = parse_date(line).unwrap();
        assert_eq!(timestamp.format("%Y-%m-%d %H:%M:%S,%f").to_string(), "2021-03-01 00:00:00,000000000");
    }

    #[test]
    fn test_parse_line() {
        let line = r#"{"asctime": "2021-03-01 00:00:00,000", "message": "test"}"#;
        let log_entry = parse_line(line).unwrap();
        assert_eq!(log_entry["asctime"].as_str().unwrap(), "2021-03-01 00:00:00,000");
        assert_eq!(log_entry["message"].as_str().unwrap(), "test");
    }
}