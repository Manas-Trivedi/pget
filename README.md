# parallel-get

parallel-get (`pget`) is a command-line HTTP file downloader implemented in Rust.

At runtime, it probes remote range support and selects the transfer strategy accordingly:

- Executes a 4-way segmented download when HTTP byte ranges are supported
- Falls back to a single-stream transfer when range requests are unavailable

The CLI provides a live progress renderer (throughput and ETA) and supports interactive output filename override prior to transfer start.

## Features

- Automatic output filename derivation from URL path
- Interactive output filename override
- Automatic range capability detection via `Range: bytes=0-0` probe
- Segmented parallel transfer mode (4 chunks)
- Single-stream fallback mode for non-range endpoints
- Live progress telemetry:
	- bytes transferred
	- effective throughput
	- estimated time to completion
- Optional verbose mode with per-chunk progress bars

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

Enable verbose per-chunk progress output:

```bash
cargo run -- https://example.com/archive.zip --verbose
```

Short-flag equivalent:

```bash
cargo run -- https://example.com/archive.zip -v
```

## CLI Behavior

1. Derives a default output filename from the input URL.
2. Prompts for optional filename override:

	```text
	Rename file? [press Enter to keep] [detected-name]:
	```

3. Probes server byte-range support.
4. Reports resolved file size and range capability.
5. Initiates one of the following transfer paths:
	- segmented parallel download (4 chunks), or
	- single-stream fallback transfer.
6. Renders progress continuously until completion.

## Usage Summary

```text
pget <url> [-v|--verbose]
```

If no URL is provided, `pget` emits:

```text
Usage: pget <url> [-v]
```

## Notes

- The current implementation uses a fixed chunk count of 4 in segmented mode.
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
