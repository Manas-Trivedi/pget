use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use std::io::SeekFrom;
use futures_util::StreamExt;
use std::io::{stdin, stdout, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::collections::VecDeque;

const PIECE_SIZE: u64 = 4 * 1024 * 1024; // 4MB

fn filename_from_url(url: &str) -> String {
    url.split('/')
        .last()
        .filter(|s| !s.is_empty())
        .unwrap_or("download.bin")
        .to_string()
}

fn ask_filename(default: &str) -> String {

    print!("Rename file? [press Enter to keep] [{}]: ", default);
    stdout().flush().unwrap();

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    let trimmed = input.trim();

    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: pget <url> [-v]");
        std::process::exit(1);
    }

    let url = &args[1];

    let default_name = filename_from_url(url);
    println!("Detected filename: {}", default_name);
    let filename = ask_filename(&default_name);

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

    println!("File size: {} bytes", file_size);
    println!("Range supported: {}", supports_range);

    // Fallback single-thread download
    if !supports_range {

        println!("Falling back to single-thread download.");

        let response = client.get(url).send().await?;
        let mut stream = response.bytes_stream();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filename)
            .await?;

        let progress = Arc::new(Mutex::new(0u64));
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

    let mut handles = vec![];

    // create range queue
    let ranges = Arc::new(Mutex::new(VecDeque::new()));
    {
        let mut q = ranges.lock().unwrap();
        let mut start = 0;
        while start < file_size {
            let end = (start + PIECE_SIZE - 1).min(file_size - 1);
            q.push_back((start, end));
            start += PIECE_SIZE;
        }
    }

    for worker_id in 0..chunks {
        let url = url.to_string();
        let client = client.clone();
        let progress = progress.clone();
        let filename = filename.clone();
        let ranges = ranges.clone();

        let handle = tokio::spawn(async move {
            loop {
                let (start, end) = {
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
            }

        });
        handles.push(handle);
    }

    for h in handles {
        h.await?;
    }

    print!("\x1b[?25h");
    stdout().flush().unwrap();

    println!("\nDownload complete");

    Ok(())
}