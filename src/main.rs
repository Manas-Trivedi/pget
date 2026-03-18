use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use std::io::SeekFrom;
use futures_util::StreamExt;
use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: fastdl <url> [-v]");
        std::process::exit(1);
    }

    let url = &args[1];
    let verbose = args.iter().any(|a| a == "-v" || a == "--verbose");

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
            .open("download.bin")
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

    // Multi-thread downloader
    let chunks: usize = 4;
    let chunk_size = file_size / chunks as u64;

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

                    for (i, chunk_bytes) in p.iter().enumerate() {

                        let percent = *chunk_bytes as f64 / chunk_size as f64;
                        let filled = (percent * 20.0).round() as usize;

                        lines.push(format!(
                            "Chunk{}: {}{}",
                            i,
                            "█".repeat(filled),
                            "░".repeat(20 - filled)
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

    let mut handles = vec![];

    for i in 0..chunks {

        let start = i as u64 * chunk_size;
        let end = if i == chunks - 1 {
            file_size - 1
        } else {
            (i as u64 + 1) * chunk_size - 1
        };

        let url = url.to_string();
        let client = client.clone();
        let progress = progress.clone();

        let handle = tokio::spawn(async move {

            let response = client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end))
                .send()
                .await
                .unwrap();

            let mut stream = response.bytes_stream();

            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open("download.bin")
                .await
                .unwrap();

            file.seek(SeekFrom::Start(start)).await.unwrap();

            while let Some(item) = stream.next().await {

                let chunk = item.unwrap();

                file.write_all(&chunk).await.unwrap();

                let mut p = progress.lock().unwrap();
                p[i] += chunk.len() as u64;
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