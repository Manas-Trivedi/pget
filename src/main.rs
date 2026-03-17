use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use std::io::SeekFrom;
use futures_util::StreamExt;
use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage: fastdl <url> [-v]");
        std::process::exit(1);
    }

    let url = &args[1];
    let client = Client::new();

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

    if !supports_range {
        println!("Server does not support range requests.");
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
        tokio::spawn(async move {
            loop {
                let downloaded = *progress_clone.lock().unwrap();
                let percent = downloaded as f64 / file_size as f64;
                let filled = (percent * 30.0).round() as usize;
                let bar = format!(
                    "{}{}",
                    "█".repeat(filled),
                    "░".repeat(30 - filled)
                );
                print!("\r\x1b[2K{}", bar);
                stdout().flush().unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        });
        println!("Download complete");
        return Ok(());
    }

    let chunks = 4;
    let chunk_size = file_size / chunks;
    let max_retries = 5usize;
    let retry_delay = tokio::time::Duration::from_millis(500);

    let verbose = std::env::args().any(|a| a == "-v" || a == "--verbose");
    let progress = Arc::new(Mutex::new(vec![0u64; chunks as usize]));
    let progress_clone = progress.clone();

    println!();

    // hide cursor
    print!("\x1b[?25l");
    stdout().flush().unwrap();

    // renderer
    tokio::spawn(async move {

        loop {

            let lines = {
                let p = progress_clone.lock().unwrap();
                let mut lines = Vec::new();

                if verbose {

                    let total: u64 = p.iter().sum();
                    let total_percent = total as f64 / file_size as f64;
                    let filled = (total_percent * 30.0).round() as usize;

                    lines.push(format!(
                        "Total : {}{}",
                        "█".repeat(filled),
                        "░".repeat(30 - filled)
                    ));
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

                    let mut bar = String::new();

                    for chunk_bytes in p.iter() {

                        let percent = *chunk_bytes as f64 / chunk_size as f64;
                        let filled = (percent * 10.0).round() as usize;

                        bar.push_str(&"█".repeat(filled));
                        bar.push_str(&"░".repeat(10 - filled));
                    }

                    lines.push(bar);
                }

                lines
            };

            for line in &lines {
                print!("\r\x1b[2K{}\n", line);
            }

            print!("\x1b[{}A", lines.len());

            stdout().flush().unwrap();

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

    });

    // create file
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open("download.bin")
        .await?;

    file.set_len(file_size).await?;

    let mut handles = vec![];

    for i in 0..chunks {

        let start = i * chunk_size;
        let end = if i == chunks - 1 {
            file_size - 1
        } else {
            (i + 1) * chunk_size - 1
        };

        let url = url.to_string();
        let client = client.clone();
        let progress = progress.clone();
        let retry_delay = retry_delay;

        let handle = tokio::spawn(async move {
            let chunk_len = end - start + 1;
            let mut downloaded = 0u64;
            let mut retries = 0usize;

            while downloaded < chunk_len {
                let range_start = start + downloaded;

                let response = match client
                    .get(&url)
                    .header("Range", format!("bytes={}-{}", range_start, end))
                    .send()
                    .await
                {
                    Ok(resp) => resp,
                    Err(err) => {
                        if retries >= max_retries {
                            return Err(format!(
                                "chunk {} failed after {} retries (request): {}",
                                i, max_retries, err
                            ));
                        }
                        retries += 1;
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                };

                let mut stream = response.bytes_stream();

                let mut file = match OpenOptions::new().write(true).open("download.bin").await {
                    Ok(file) => file,
                    Err(err) => {
                        if retries >= max_retries {
                            return Err(format!(
                                "chunk {} failed after {} retries (open file): {}",
                                i, max_retries, err
                            ));
                        }
                        retries += 1;
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                };

                if let Err(err) = file.seek(SeekFrom::Start(range_start)).await {
                    if retries >= max_retries {
                        return Err(format!(
                            "chunk {} failed after {} retries (seek): {}",
                            i, max_retries, err
                        ));
                    }
                    retries += 1;
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }

                let mut failed_this_attempt = false;

                while let Some(item) = stream.next().await {
                    let chunk = match item {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            failed_this_attempt = true;
                            break;
                        }
                    };

                    if let Err(_) = file.write_all(&chunk).await {
                        failed_this_attempt = true;
                        break;
                    }

                    downloaded += chunk.len() as u64;

                    let mut p = progress.lock().unwrap();
                    p[i as usize] += chunk.len() as u64;
                }

                if failed_this_attempt {
                    if retries >= max_retries {
                        return Err(format!(
                            "chunk {} failed after {} retries (stream/write)",
                            i, max_retries
                        ));
                    }
                    retries += 1;
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }

                retries = 0;
            }

            Ok::<(), String>(())
        });

        handles.push(handle);
    }

    for h in handles {
        h.await
            .map_err(|e| std::io::Error::other(format!("Chunk task join error: {e}")))?
            .map_err(std::io::Error::other)?;
    }

    // show cursor again
    print!("\x1b[?25h");
    stdout().flush().unwrap();

    println!();
    println!("Download complete");

    Ok(())
}