# parallel-get

parallel-get (`pget`) is a command-line HTTP file downloader implemented in Rust.

At runtime, it probes remote range support and selects the transfer strategy accordingly:

- Executes segmented parallel downloads when HTTP byte ranges are supported (default: 4 workers)
- Falls back to a single-stream transfer when range requests are unavailable

The CLI provides a live progress renderer (throughput and ETA), interactive output filename override prior to transfer start, and resumable segmented transfers.

## Features

- Automatic output filename derivation from URL path
- Interactive output filename override
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

1. Derives a default output filename from the input URL.
2. Prompts for optional filename override:

	```text
	Rename file? [press Enter to keep] [detected-name]:
	```

3. Probes server byte-range support.
4. Reports resolved file size and range capability.
5. If segmented mode is selected, initializes a `4 MiB` piece queue and dispatches pieces across `N` workers (`-t/--threads`).
6. If interrupted in segmented mode, persists completed piece indices to `<filename>.pget`.
7. On restart, reloads sidecar state and skips completed pieces.
8. Renders progress continuously until completion and removes the sidecar state file when done.

## Usage Summary

```text
pget <url> [-v|--verbose] [-t|--threads <n>]
```

If no URL is provided, `pget` emits:

```text
Usage: pget <url> [-v]
```

The built-in usage output is currently minimal and does not enumerate all optional flags.

## Notes

- Default worker count is `4` when `-t/--threads` is not specified.
- Segmented mode uses fixed-size `4 MiB` pieces and a shared queue for work distribution.
- Resume state is stored in `<filename>.pget` and cleaned up after successful completion.
- In segmented mode, the output file is preallocated to enable random-access writes.
- The progress renderer updates in-place and temporarily hides the terminal cursor during transfer.
- The CLI is currently interactive due to the filename override prompt.

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
