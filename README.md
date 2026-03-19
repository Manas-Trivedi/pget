# parallel-get

parallel-get (`pget`) is a command-line HTTP file downloader implemented in Rust.

At runtime, it probes remote range support and selects the transfer strategy accordingly:

- Executes segmented parallel downloads when HTTP byte ranges are supported (default: 4 workers)
- Falls back to a single-stream transfer when range requests are unavailable

The CLI provides a live progress renderer (throughput and ETA), interactive or non-interactive output selection, and resumable segmented transfers.

## Features

- Automatic output filename derivation from URL path
- Interactive output filename override with validation and overwrite confirmation
- Non-interactive filename selection via `--output/-o` and `--no-prompt`
- Automatic range capability detection via `Range: bytes=0-0` probe
- Segmented parallel transfer mode with configurable worker count
- Single-stream fallback mode for non-range endpoints
- Piece-based scheduling (`4 MiB` pieces) for balanced worker utilization
- Resume support for segmented downloads via persisted sidecar state (`<filename>.pget`)
- Interrupt handling (`Ctrl+C`) with on-exit progress state persistence
- Live progress telemetry:
	- bytes transferred
	- effective throughput
	- estimated time to completion
- Optional verbose mode with per-worker progress bars

## Requirements

- Rust toolchain (stable)
- Cargo
- Network access to the target URL

## Build

```bash
cargo build --release
```

Binary path after build:

```bash
./target/release/pget
```

## Install

Prebuilt binaries are available from GitHub Releases.

### Linux (x86_64)

```bash
curl -L https://github.com/Manas-Trivedi/pget/releases/latest/download/pget-linux-x86_64 \
	-o pget

chmod +x pget
sudo mv pget /usr/local/bin/
```

### macOS (Intel)

```bash
curl -L https://github.com/Manas-Trivedi/pget/releases/latest/download/pget-macos-x86_64 \
	-o pget

chmod +x pget
sudo mv pget /usr/local/bin/
```

### macOS (Apple Silicon)

```bash
curl -L https://github.com/Manas-Trivedi/pget/releases/latest/download/pget-macos-arm64 \
	-o pget

chmod +x pget
sudo mv pget /usr/local/bin/
```

### Windows (PowerShell)

```powershell
curl.exe -L https://github.com/Manas-Trivedi/pget/releases/latest/download/pget-windows-x86_64.exe -o pget.exe
```

Then add the directory containing `pget.exe` to your `PATH`, or run it directly from the download location.

### Verify installation

macOS / Linux:

```bash
which pget
pget --help
```

Windows (PowerShell):

```powershell
Get-Command pget
pget --help
```

## Run

```bash
cargo run -- <url>
```

Example:

```bash
cargo run -- https://example.com/archive.zip
```

Enable verbose per-worker progress output:

```bash
cargo run -- https://example.com/archive.zip --verbose
```

Short-flag equivalent:

```bash
cargo run -- https://example.com/archive.zip -v
```

Print the installed version:

```bash
cargo run -- --version
```

Use the detected filename without prompting:

```bash
cargo run -- https://example.com/archive.zip --no-prompt
```

Write to an explicit output path:

```bash
cargo run -- https://example.com/archive.zip --output ./downloads/archive.zip
```

Short-flag equivalent:

```bash
cargo run -- https://example.com/archive.zip -o ./downloads/archive.zip
```

Specify worker count:

```bash
cargo run -- https://example.com/archive.zip --threads 8
```

Short-flag equivalent:

```bash
cargo run -- https://example.com/archive.zip -t 8
```

Resume a previously interrupted segmented download:

```bash
cargo run -- https://example.com/archive.zip --threads 8
```

If a matching `<filename>.pget` state file exists, `pget` reloads completed piece indices and continues from remaining ranges.

## CLI Behavior

1. Derives a default output filename from the input URL unless `-o/--output` is provided.
2. Prompts for output filename with guided rename UX unless `--no-prompt` or `-o/--output` is used:

	```text
	Output filename
	  Press Enter to keep: detected-name
	  Type a new filename to rename
	Save as [detected-name]:
	```

3. If `--no-prompt` is used, `pget` accepts the detected filename automatically.
4. If `-o/--output` is used, `pget` writes to that exact path.
5. If the target file already exists and a matching sidecar state file exists, `pget` resumes automatically.
6. If the target file already exists without resumable state:
   - interactive mode asks for overwrite confirmation
   - `--no-prompt` exits safely instead of overwriting
7. Probes server byte-range support.
8. Reports resolved file size and range capability.
9. If segmented mode is selected, initializes a `4 MiB` piece queue and dispatches pieces across `N` workers (`-t/--threads`).
10. If interrupted in segmented mode, persists completed piece indices to `<filename>.pget`.
11. On restart, reloads sidecar state and skips completed pieces.
12. Renders progress continuously until completion and removes the sidecar state file when done.

Rename prompt behavior:

- Empty input keeps the detected filename.
- Invalid names (for example `.`, `..`, directory paths, or names containing `/`) are rejected and re-prompted.
- If the target file already exists and no matching state file is present, `pget` asks for overwrite confirmation (`[y/N]`).
- If both target file and matching state file exist, `pget` assumes resume mode for that filename.

## Usage Summary

```text
pget <url> [options]
```

Current built-in help output:

```text
pget 0.1.0
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
```

Flag note:

- `-v` remains the short flag for verbose mode.
- Version is exposed as `--version` only to avoid colliding with verbose.

## Notes

- Default worker count is `4` when `-t/--threads` is not specified.
- Segmented mode uses fixed-size `4 MiB` pieces and a shared queue for work distribution.
- Resume state is stored in `<filename>.pget` and cleaned up after successful completion.
- In segmented mode, the output file is preallocated to enable random-access writes.
- The progress renderer updates in-place and temporarily hides the terminal cursor during transfer.
- `--output/-o` may include a path, not just a bare filename.
- `--no-prompt` is automation-friendly but intentionally refuses to overwrite an existing non-resumable file.

## Development

Run with the debug profile:

```bash
cargo run -- <url>
```

Run formatting and lint checks:

```bash
cargo fmt
cargo clippy --all-targets --all-features
```

## Todo

- [x] Improve CLI ergonomics: add `--output/-o`, `--no-prompt`, and fuller `--help`
- [ ] Harden reliability: retries with backoff, request timeouts, and graceful error handling
- [ ] Strengthen resume safety: store/validate `ETag` or `Last-Modified` in `.pget` state
- [ ] Add integrity checks: optional `--checksum` verification after download
- [ ] Expand transfer controls: `--limit-rate`, overwrite/skip policies, explicit continue modes
- [ ] Support auth/customization: custom headers, bearer/basic auth, and proxy support
- [ ] Improve automation UX: `--quiet`, `--json-progress`, and cleaner verbose logs
- [ ] Add test + CI coverage: range/no-range, resume, retries, and interruption scenarios
