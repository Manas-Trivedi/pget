use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use std::io::SeekFrom;
use futures_util::StreamExt;
use std::io::{stdin, stdout, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::collections::{VecDeque, HashSet};
use std::path::Path;
use colored::*;

const PIECE_SIZE: u64 = 4 * 1024 * 1024; // 4MB

fn log(msg: &str) {
    println!("{} {}", "pget".bright_cyan().bold(), msg);
}

fn filename_from_url(url: &str) -> String {
    url.split('/')
        .last()
        .filter(|s| !s.is_empty())
        .unwrap_or("download.bin")
        .to_string()
}

fn ask_filename(default: &str) -> String {
    println!("{}", "\nOutput filename".bright_white().bold());
    println!("  Press Enter to keep: {}", default.bright_green());
    println!("  Type a new filename to rename");

    loop {
        print!("Save as [{}]: ", default);
        stdout().flush().unwrap();

        let mut input = String::new();
        stdin().read_line(&mut input).unwrap();

        let candidate = if input.trim().is_empty() {
            default.to_string()
        } else {
            input.trim().to_string()
        };

        if candidate == "." || candidate == ".." {
            println!("{}", "Please enter a valid filename.".yellow());
            continue;
        }

        if candidate.contains('/') || candidate.contains('\0') {
            println!("{}", "Filename cannot contain '/' or null characters.".yellow());
            continue;
        }

        let path = Path::new(&candidate);
        if path.is_dir() {
            println!("{}", "That name points to a directory. Choose a file name.".yellow());
            continue;
        }

        if path.exists() {
            let meta_file = format!("{}.pget", candidate);
            if Path::new(&meta_file).exists() {
                println!("{}", "Found resumable state for this file. Will resume.".bright_cyan());
                return candidate;
            }

            print!("File already exists. Overwrite? [y/N]: ");
            stdout().flush().unwrap();

            let mut confirm = String::new();
            stdin().read_line(&mut confirm).unwrap();

            if matches!(confirm.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
                return candidate;
            }

            println!("{}", "Okay, choose a different filename.".yellow());
            continue;
        }

        return candidate;
    }
}

fn parse_threads(args: &[String]) -> usize {
    let mut threads = 4;

    for i in 0..args.len() {
        if args[i] == "-t" || args[i] == "--threads" {
            if let Some(v) = args.get(i + 1) {
                threads = v.parse().unwrap_or(4);
            }
        }
    }

    threads.max(1)
}

fn save_state(meta_file: &str, completed: &HashSet<u64>, file_size: u64) {

    let mut list: Vec<String> = completed.iter().map(|v| v.to_string()).collect();
    list.sort();

    let data = format!(
        "file_size={}\npiece_size={}\ncompleted={}",
        file_size,
        PIECE_SIZE,
        list.join(",")
    );

    let _ = std::fs::write(meta_file, data);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: pget <url> [-v]");
        std::process::exit(1);
    }

    let url = &args[1];

    let default_name = filename_from_url(url);
    log(&format!("Detected filename: {}", default_name));
    let filename = ask_filename(&default_name);

    let meta_file = format!("{}.pget", filename);
    let completed = Arc::new(Mutex::new(HashSet::<u64>::new()));

    let verbose = args.iter().any(|a| a == "-v" || a == "--verbose");
    let chunks = parse_threads(&args);

    let client = Client::new();

    // Probe server with range request
    let resp = client
        .get(url)
        .header("Range", "bytes=0-0")
        .send()
        .await?;

    let supports_range = resp.status() == reqwest::StatusCode::PARTIAL_CONTENT;

    let file_size = if supports_range {

        let content_range = resp
            .headers()
            .get("content-range")
            .ok_or("Missing Content-Range")?
            .to_str()?;

        content_range
            .split('/')
            .nth(1)
            .ok_or("Invalid Content-Range")?
            .parse::<u64>()?

    } else {

        resp.headers()
            .get("content-length")
            .ok_or("Missing Content-Length")?
            .to_str()?
            .parse::<u64>()?

    };

    log(&format!("File size: {} bytes", file_size));
    log(&format!("Range supported: {}", supports_range));

    // load previous state if exists
    if let Ok(content) = std::fs::read_to_string(&meta_file) {
        log("Resuming previous download");
        for line in content.lines() {
            if line.starts_with("completed=") {
                let pieces = line.replace("completed=", "");
                for p in pieces.split(',') {
                    if let Ok(id) = p.parse::<u64>() {
                        completed.lock().unwrap().insert(id);
                    }
                }
            }
        }
    }

    let completed_count = completed.lock().unwrap().len() as u64;
    let resumed_bytes = completed_count * PIECE_SIZE;

    if resumed_bytes > 0 {
        log(&format!("Resumed: {:.1} MB", resumed_bytes as f64 / 1_000_000.0));
    }

    // Fallback single-thread download
    if !supports_range {

        log("Server does not support range requests — falling back to single-thread download");

        let response = client.get(url).send().await?;
        let mut stream = response.bytes_stream();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filename)
            .await?;

        let progress = Arc::new(Mutex::new(resumed_bytes));
        let progress_clone = progress.clone();

        print!("\x1b[?25l");
        stdout().flush().unwrap();

        let start_time = Instant::now();

        tokio::spawn(async move {

            loop {

                let downloaded = *progress_clone.lock().unwrap();
                let elapsed = start_time.elapsed().as_secs_f64();

                let percent = downloaded as f64 / file_size as f64;
                let filled = (percent * 30.0).round() as usize;

                let speed = downloaded as f64 / elapsed;
                let remaining = file_size - downloaded;

                let eta = if speed > 0.0 {
                    remaining as f64 / speed
                } else {
                    0.0
                };

                let bar = format!(
                    "{}{}  {:.1}MB / {:.1}MB  {:.1}MB/s  ETA {:.0}s",
                    "█".repeat(filled),
                    "░".repeat(30 - filled),
                    downloaded as f64 / 1_000_000.0,
                    file_size as f64 / 1_000_000.0,
                    speed / 1_000_000.0,
                    eta
                );

                print!("\r\x1b[2K{}", bar);
                stdout().flush().unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }

        });

        while let Some(item) = stream.next().await {

            let chunk = item?;
            file.write_all(&chunk).await?;

            let mut p = progress.lock().unwrap();
            *p += chunk.len() as u64;
        }

        print!("\x1b[?25h");
        stdout().flush().unwrap();

        println!("\nDownload complete");

        return Ok(());
    }

    let progress = Arc::new(Mutex::new(vec![0u64; chunks]));
    {
        let mut p = progress.lock().unwrap();
        p[0] = resumed_bytes;
    }
    let progress_clone = progress.clone();

    let start_time = Instant::now();

    println!();
    print!("\x1b[?25l");
    stdout().flush().unwrap();

    // Renderer
    tokio::spawn(async move {
        loop {
            let elapsed = start_time.elapsed().as_secs_f64();
            let lines = {
                let p = progress_clone.lock().unwrap();
                let mut lines = Vec::new();

                let total_downloaded: u64 = p.iter().sum();

                let percent = total_downloaded as f64 / file_size as f64;
                let filled = (percent * 30.0).round() as usize;

                let speed = total_downloaded as f64 / elapsed;
                let remaining = file_size - total_downloaded;

                let eta = if speed > 0.0 {
                    remaining as f64 / speed
                } else {
                    0.0
                };

                let bar = format!(
                    "{}{}  {:.1}MB / {:.1}MB  {:.1}MB/s  ETA {:.0}s",
                    "█".repeat(filled),
                    "░".repeat(30 - filled),
                    total_downloaded as f64 / 1_000_000.0,
                    file_size as f64 / 1_000_000.0,
                    speed / 1_000_000.0,
                    eta
                );

                if verbose {
                    lines.push(bar);
                    lines.push(String::new());
                    for (i, worker_bytes) in p.iter().enumerate() {
                        let percent = if total_downloaded > 0 {
                            *worker_bytes as f64 / total_downloaded as f64
                        } else {
                            0.0
                        };
                        let filled = (percent * 20.0).round() as usize;
                        lines.push(format!(
                            "Worker{}: {}{}  {:.1}MB",
                            i,
                            "█".repeat(filled),
                            "░".repeat(20 - filled),
                            *worker_bytes as f64 / 1_000_000.0
                        ));
                        lines.push(String::new());
                    }

                } else {

                    lines.push(bar);

                }

                lines
            };

            for line in &lines {
                print!("\r\x1b[2K{}\n", line);
            }

            print!("\x1b[{}A", lines.len());
            stdout().flush().unwrap();

            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

    });

    // create file and preallocate size
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&filename)
        .await?;

    file.set_len(file_size).await?;

    let completed_ctrl = completed.clone();
    let meta_ctrl = meta_file.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!();
        log("Interrupted — saving progress");
        save_state(&meta_ctrl, &completed_ctrl.lock().unwrap(), file_size);
        print!("\x1b[?25h");
        stdout().flush().unwrap();
        std::process::exit(0);
    });

    let mut handles = vec![];

    // create range queue
    let ranges = Arc::new(Mutex::new(VecDeque::<(u64,u64,u64)>::new()));
    {
        let mut q = ranges.lock().unwrap();
        let mut start = 0;
        let mut piece_index = 0;
        let completed_guard = completed.lock().unwrap();
        while start < file_size {
            let end = (start + PIECE_SIZE - 1).min(file_size - 1);
            if !completed_guard.contains(&piece_index) {
                q.push_back((piece_index, start, end));
            }
            start += PIECE_SIZE;
            piece_index += 1;
        }
    }

    for worker_id in 0..chunks {
        let url = url.to_string();
        let client = client.clone();
        let progress = progress.clone();
        let filename = filename.clone();
        let ranges = ranges.clone();
        let completed = completed.clone();
        let meta_file = meta_file.clone();

        let handle = tokio::spawn(async move {
            loop {
                let (piece_index, start, end) = {
                    let mut q = ranges.lock().unwrap();
                    match q.pop_front() {
                        Some(r) => r,
                        None => break,
                    }
                };
                let response = client
                    .get(&url)
                    .header("Range", format!("bytes={}-{}", start, end))
                    .send()
                    .await
                    .unwrap();

                let mut stream = response.bytes_stream();
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(&filename)
                    .await
                    .unwrap();

                file.seek(SeekFrom::Start(start)).await.unwrap();
                while let Some(item) = stream.next().await {
                    let chunk = item.unwrap();
                    file.write_all(&chunk).await.unwrap();
                    let mut p = progress.lock().unwrap();
                    p[worker_id] += chunk.len() as u64;
                }
                completed.lock().unwrap().insert(piece_index);
                save_state(&meta_file, &completed.lock().unwrap(), file_size);
            }

        });
        handles.push(handle);
    }

    for h in handles {
        h.await?;
    }

    print!("\x1b[?25h");
    stdout().flush().unwrap();

    let _ = std::fs::remove_file(meta_file);

    println!("\nDownload complete");

    Ok(())
}