use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use std::io::SeekFrom;
use futures_util::StreamExt;
use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let url = "https://nbg1-speed.hetzner.com/100MB.bin";
    let client = Client::new();

    let resp = client
        .get(url)
        .header("Range", "bytes=0-0")
        .send()
        .await?;

    let content_range = resp
        .headers()
        .get("content-range")
        .ok_or("Missing Content-Range")?
        .to_str()?;

    let file_size: u64 = content_range
        .split('/')
        .nth(1)
        .ok_or("Invalid Content-Range")?
        .parse()?;

    println!("File size: {} bytes", file_size);

    let chunks = 4;
    let chunk_size = file_size / chunks;

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

        let handle = tokio::spawn(async move {

            let response = client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end))
                .send()
                .await
                .unwrap();

            let mut stream = response.bytes_stream();

            let mut file = OpenOptions::new()
                .write(true)
                .open("download.bin")
                .await
                .unwrap();

            file.seek(SeekFrom::Start(start)).await.unwrap();

            while let Some(item) = stream.next().await {

                let chunk = item.unwrap();

                file.write_all(&chunk).await.unwrap();

                let mut p = progress.lock().unwrap();
                p[i as usize] += chunk.len() as u64;
            }

        });

        handles.push(handle);
    }

    for h in handles {
        h.await?;
    }

    // show cursor again
    print!("\x1b[?25h");
    stdout().flush().unwrap();

    println!();
    println!("Download complete");

    Ok(())
}