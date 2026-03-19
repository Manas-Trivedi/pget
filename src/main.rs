use colored::*;
use futures_util::StreamExt;
use reqwest::Client;
use std::collections::{HashSet, VecDeque};
use std::io::SeekFrom;
use std::io::{Error as IoError, ErrorKind};
use std::io::{Write, stdin, stdout};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::time::timeout;

const PIECE_SIZE: u64 = 4 * 1024 * 1024; // 4MB
const VERSION: &str = env!("CARGO_PKG_VERSION");
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: usize = 3;
const RETRY_BACKOFF_BASE_MS: u64 = 500;

type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

fn print_help() {
    println!(
        "\
pget {version}
Parallel HTTP downloader with resumable segmented transfers.

Usage:
  pget <url> [options]
  pget --help
  pget --version

Arguments:
  <url>                 Download source URL

Options:
  -o, --output <path>   Write to the given output path
      --no-prompt       Use the detected filename without prompting
  -t, --threads <n>     Number of worker threads for range downloads (default: 4)
  -v, --verbose         Show per-worker progress output
      --help            Show this help text
      --version         Show version information

Notes:
  - `-v` is reserved for verbose mode.
  - Use `--version` for the program version.
  - `--no-prompt` refuses to overwrite an existing non-resumable file.

Examples:
  pget https://example.com/archive.zip
  pget https://example.com/archive.zip --no-prompt
  pget https://example.com/archive.zip -o ./downloads/archive.zip
  pget https://example.com/archive.zip -t 8 -v",
        version = VERSION
    );
}

struct CliArgs {
    url: String,
    output: Option<PathBuf>,
    no_prompt: bool,
    threads: usize,
    verbose: bool,
}

fn parse_args(args: &[String]) -> Result<CliArgs, String> {
    let mut url: Option<String> = None;
    let mut output: Option<PathBuf> = None;
    let mut no_prompt = false;
    let mut threads = 4usize;
    let mut verbose = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--version" => {
                log(&format!("{}", VERSION));
                std::process::exit(0);
            }
            "-v" | "--verbose" => {
                verbose = true;
                i += 1;
            }
            "--no-prompt" => {
                no_prompt = true;
                i += 1;
            }
            "-t" | "--threads" => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| "Missing value for --threads".to_string())?;
                threads = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid thread count: {value}"))?
                    .max(1);
                i += 2;
            }
            "-o" | "--output" => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| "Missing value for --output".to_string())?;
                output = Some(PathBuf::from(value));
                i += 2;
            }
            value if value.starts_with('-') => {
                return Err(format!("Unknown flag: {value}"));
            }
            value => {
                if url.is_some() {
                    return Err(format!("Unexpected positional argument: {value}"));
                }
                url = Some(value.to_string());
                i += 1;
            }
        }
    }

    let url = url.ok_or_else(|| "Missing required <url> argument".to_string())?;

    Ok(CliArgs {
        url,
        output,
        no_prompt,
        threads,
        verbose,
    })
}

fn validate_output_path(path: &Path) -> Result<(), String> {
    if path == Path::new(".") || path == Path::new("..") {
        return Err("Please enter a valid file path.".to_string());
    }

    if path.as_os_str().is_empty() {
        return Err("Output path cannot be empty.".to_string());
    }

    if path.is_dir() {
        return Err("That path points to a directory. Choose a file path.".to_string());
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(format!(
                "Parent directory does not exist: {}",
                parent.display()
            ));
        }
    }

    Ok(())
}

fn prepare_output_path(path: PathBuf, allow_prompt: bool) -> Result<PathBuf, String> {
    validate_output_path(&path)?;

    let meta_path = PathBuf::from(format!("{}.pget", path.display()));

    if path.exists() {
        if meta_path.exists() {
            println!(
                "{}",
                "Found resumable state for this file. Will resume.".bright_cyan()
            );
            return Ok(path);
        }

        if !allow_prompt {
            return Err(format!(
                "Refusing to overwrite existing file without confirmation: {}",
                path.display()
            ));
        }

        print!("File already exists. Overwrite? [y/N]: ");
        stdout().flush().unwrap();

        let mut confirm = String::new();
        stdin().read_line(&mut confirm).unwrap();

        if matches!(confirm.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            return Ok(path);
        }

        return Err("Okay, choose a different filename.".to_string());
    }

    Ok(path)
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

        if candidate.contains('/') || candidate.contains('\0') {
            println!(
                "{}",
                "Filename cannot contain '/' or null characters.".yellow()
            );
            continue;
        }

        match prepare_output_path(PathBuf::from(&candidate), true) {
            Ok(path) => return path.display().to_string(),
            Err(err) => {
                println!("{}", err.yellow());
                continue;
            }
        }
    }
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

fn app_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(IoError::other(message.into()))
}

fn retry_delay(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5) as u32;
    Duration::from_millis(RETRY_BACKOFF_BASE_MS.saturating_mul(2u64.pow(exponent)))
}

fn timeout_error(context: &str) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(IoError::new(
        ErrorKind::TimedOut,
        format!("{context} timed out after {}s", REQUEST_TIMEOUT.as_secs()),
    ))
}

async fn send_request(
    client: &Client,
    url: &str,
    range: Option<(u64, u64)>,
    context: &str,
) -> AppResult<reqwest::Response> {
    let mut last_error: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for attempt in 1..=MAX_RETRIES {
        let mut request = client.get(url);
        if let Some((start, end)) = range {
            request = request.header("Range", format!("bytes={start}-{end}"));
        }

        match timeout(REQUEST_TIMEOUT, request.send()).await {
            Ok(Ok(response)) => match response.error_for_status() {
                Ok(response) => return Ok(response),
                Err(err) => last_error = Some(Box::new(err)),
            },
            Ok(Err(err)) => last_error = Some(Box::new(err)),
            Err(_) => last_error = Some(timeout_error(context)),
        }

        if attempt < MAX_RETRIES {
            let delay = retry_delay(attempt);
            log(&format!(
                "{context} failed (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                delay.as_secs_f64()
            ));
            tokio::time::sleep(delay).await;
        }
    }

    Err(last_error.unwrap_or_else(|| app_error(format!("{context} failed"))))
}

async fn probe_server(client: &Client, url: &str) -> AppResult<(bool, u64)> {
    let resp = send_request(client, url, Some((0, 0)), "Range probe").await?;
    let supports_range = resp.status() == reqwest::StatusCode::PARTIAL_CONTENT;

    let file_size = if supports_range {
        let content_range = resp
            .headers()
            .get("content-range")
            .ok_or_else(|| app_error("Missing Content-Range"))?
            .to_str()?;

        content_range
            .split('/')
            .nth(1)
            .ok_or_else(|| app_error("Invalid Content-Range"))?
            .parse::<u64>()?
    } else {
        resp.headers()
            .get("content-length")
            .ok_or_else(|| app_error("Missing Content-Length"))?
            .to_str()?
            .parse::<u64>()?
    };

    Ok((supports_range, file_size))
}

async fn download_single_stream(
    client: &Client,
    url: &str,
    filename: &str,
    file_size: u64,
    progress: Arc<Mutex<u64>>,
) -> AppResult<()> {
    let mut last_error: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for attempt in 1..=MAX_RETRIES {
        {
            let mut current = progress.lock().unwrap();
            *current = 0;
        }

        let response = match send_request(client, url, None, "Single-stream request").await {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(err);
                if attempt < MAX_RETRIES {
                    let delay = retry_delay(attempt);
                    log(&format!(
                        "Single-stream download failed (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                        delay.as_secs_f64()
                    ));
                    tokio::time::sleep(delay).await;
                    continue;
                }
                break;
            }
        };

        let mut stream = response.bytes_stream();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(filename)
            .await?;

        let mut failed = None;

        loop {
            match timeout(REQUEST_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if let Err(err) = file.write_all(&chunk).await {
                        failed = Some(Box::new(err) as Box<dyn std::error::Error + Send + Sync>);
                        break;
                    }

                    let mut current = progress.lock().unwrap();
                    *current += chunk.len() as u64;
                }
                Ok(Some(Err(err))) => {
                    failed = Some(Box::new(err));
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    failed = Some(timeout_error("Single-stream response body"));
                    break;
                }
            }
        }

        if let Some(err) = failed {
            last_error = Some(err);
            if attempt < MAX_RETRIES {
                let delay = retry_delay(attempt);
                log(&format!(
                    "Single-stream download stalled or failed (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                    delay.as_secs_f64()
                ));
                tokio::time::sleep(delay).await;
                continue;
            }
            break;
        }

        let downloaded = *progress.lock().unwrap();
        if downloaded == file_size {
            return Ok(());
        }

        last_error = Some(app_error(format!(
            "Single-stream download ended early ({downloaded}/{file_size} bytes)"
        )));

        if attempt < MAX_RETRIES {
            let delay = retry_delay(attempt);
            log(&format!(
                "Single-stream download ended early (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                delay.as_secs_f64()
            ));
            tokio::time::sleep(delay).await;
        }
    }

    Err(last_error.unwrap_or_else(|| app_error("Single-stream download failed")))
}

async fn download_piece(
    client: &Client,
    url: &str,
    filename: &str,
    worker_id: usize,
    progress: Arc<Mutex<Vec<u64>>>,
    piece_index: u64,
    start: u64,
    end: u64,
) -> AppResult<()> {
    let expected_len = end - start + 1;
    let mut last_error: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for attempt in 1..=MAX_RETRIES {
        let context = format!("Worker {worker_id} piece {piece_index}");
        let response = match send_request(client, url, Some((start, end)), &context).await {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(err);
                if attempt < MAX_RETRIES {
                    let delay = retry_delay(attempt);
                    log(&format!(
                        "{context} failed (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                        delay.as_secs_f64()
                    ));
                    tokio::time::sleep(delay).await;
                    continue;
                }
                break;
            }
        };

        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            last_error = Some(app_error(format!(
                "{context} expected 206 Partial Content, got {}",
                response.status()
            )));
            if attempt < MAX_RETRIES {
                let delay = retry_delay(attempt);
                log(&format!(
                    "{context} returned an unexpected response (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                    delay.as_secs_f64()
                ));
                tokio::time::sleep(delay).await;
                continue;
            }
            break;
        }

        let mut stream = response.bytes_stream();
        let mut file = OpenOptions::new().write(true).open(filename).await?;
        file.seek(SeekFrom::Start(start)).await?;

        let mut downloaded_this_attempt = 0u64;
        let mut failed = None;

        loop {
            match timeout(REQUEST_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if let Err(err) = file.write_all(&chunk).await {
                        failed = Some(Box::new(err) as Box<dyn std::error::Error + Send + Sync>);
                        break;
                    }

                    downloaded_this_attempt += chunk.len() as u64;
                    let mut p = progress.lock().unwrap();
                    p[worker_id] += chunk.len() as u64;
                }
                Ok(Some(Err(err))) => {
                    failed = Some(Box::new(err));
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    failed = Some(timeout_error(&format!("{context} body")));
                    break;
                }
            }
        }

        if failed.is_none() && downloaded_this_attempt == expected_len {
            return Ok(());
        }

        {
            let mut p = progress.lock().unwrap();
            p[worker_id] = p[worker_id].saturating_sub(downloaded_this_attempt);
        }

        last_error = Some(failed.unwrap_or_else(|| {
            app_error(format!(
                "{context} ended early ({downloaded_this_attempt}/{expected_len} bytes)"
            ))
        }));

        if attempt < MAX_RETRIES {
            let delay = retry_delay(attempt);
            log(&format!(
                "{context} was incomplete (attempt {attempt}/{MAX_RETRIES}). Retrying in {:.1}s...",
                delay.as_secs_f64()
            ));
            tokio::time::sleep(delay).await;
        }
    }

    Err(last_error
        .unwrap_or_else(|| app_error(format!("Worker {worker_id} piece {piece_index} failed"))))
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let cli = match parse_args(&args) {
        Ok(cli) => cli,
        Err(err) => {
            eprintln!("Error: {err}\n");
            print_help();
            std::process::exit(1);
        }
    };

    let url = cli.url.clone();
    let default_name = filename_from_url(&url);
    log(&format!("Detected filename: {}", default_name));
    let filename = if let Some(path) = cli.output.clone() {
        prepare_output_path(path, !cli.no_prompt)?
            .display()
            .to_string()
    } else if cli.no_prompt {
        prepare_output_path(PathBuf::from(&default_name), false)?
            .display()
            .to_string()
    } else {
        ask_filename(&default_name)
    };

    let meta_file = format!("{}.pget", filename);
    let completed = Arc::new(Mutex::new(HashSet::<u64>::new()));

    let verbose = cli.verbose;
    let chunks = cli.threads;

    let client = Client::builder().connect_timeout(CONNECT_TIMEOUT).build()?;

    let (supports_range, file_size) = probe_server(&client, &url).await?;

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
        log(&format!(
            "Resumed: {:.1} MB",
            resumed_bytes as f64 / 1_000_000.0
        ));
    }

    // Fallback single-thread download
    if !supports_range {
        log("Server does not support range requests — falling back to single-thread download");

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

        if let Err(err) =
            download_single_stream(&client, &url, &filename, file_size, progress).await
        {
            print!("\x1b[?25h");
            stdout().flush().unwrap();
            return Err(app_error(format!("Download failed: {err}")));
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
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!();
                log("Interrupted — saving progress");
                save_state(&meta_ctrl, &completed_ctrl.lock().unwrap(), file_size);
                print!("\x1b[?25h");
                stdout().flush().unwrap();
                std::process::exit(0);
            }
            Err(err) => {
                eprintln!("Failed to listen for Ctrl+C: {err}");
            }
        }
    });

    let mut handles = vec![];

    // create range queue
    let ranges = Arc::new(Mutex::new(VecDeque::<(u64, u64, u64)>::new()));
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
                download_piece(
                    &client,
                    &url,
                    &filename,
                    worker_id,
                    progress.clone(),
                    piece_index,
                    start,
                    end,
                )
                .await?;

                completed.lock().unwrap().insert(piece_index);
                save_state(&meta_file, &completed.lock().unwrap(), file_size);
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }

    while let Some(handle) = handles.pop() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                for pending in handles {
                    pending.abort();
                }
                save_state(&meta_file, &completed.lock().unwrap(), file_size);
                print!("\x1b[?25h");
                stdout().flush().unwrap();
                return Err(app_error(format!(
                    "Download failed after retries. Saved progress to {}: {err}",
                    meta_file
                )));
            }
            Err(err) => {
                for pending in handles {
                    pending.abort();
                }
                save_state(&meta_file, &completed.lock().unwrap(), file_size);
                print!("\x1b[?25h");
                stdout().flush().unwrap();
                return Err(app_error(format!(
                    "A worker task crashed. Saved progress to {}: {err}",
                    meta_file
                )));
            }
        }
    }

    print!("\x1b[?25h");
    stdout().flush().unwrap();

    let _ = std::fs::remove_file(meta_file);

    println!("\nDownload complete");

    Ok(())
}
