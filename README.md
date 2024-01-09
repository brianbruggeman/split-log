# split-log

Simple log parser

## Motivation

We had a very large (62GB) log file that needed to be parsed.  Each log line was a structured json that had an `asctime` field which contained the timestamp of the log line.  The log spanned years of data; this tool sharded the log data into daily files.

Rust was chosen here because it is performant for this type of task - parsing a huge file and writing out to many files.

### Approach

The approach taken was to use a `BufReader` to read the file line by line.  Each line was parsed into a `serde_json::Value` and the `asctime` field was extracted.  The `asctime` field was then parsed into a `chrono::DateTime` and the date was extracted.  The log line was then written to a file named after the date.

Initially picked tokio with async_compression, but couldn't functionally make that work, so dropped back to a synchronous approach.

### Follow-on

This tool was sufficient for the work at hand, and given adequate log rotation, wouldn't need to be updated.  However, this could be used as a starting point for a more general purpose log extraction tool.

## Installation

While a simple `cargo install --path .` would work, this particular tool was installed onto a remote machine using `scp` and a different architecture.  To run the right commands, `x-compile` was created to cross compile the binary for the remote machine since the development machine was an M1 and the target machine was an x86_64.

## Usage

```bash
split-log --help
```

### Example:

split-log --input /path/to/big/file.json.1 --output /path/to/output/dir/with/filename_prefix

