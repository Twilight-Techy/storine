---
title: Installation
description: Install the four Storine programs with one command, from a release archive, or from source.
---

## With the install script

On Linux or macOS:

```bash tab=Linux and macOS
curl -fsSL https://github.com/Twilight-Techy/storine/releases/latest/download/install.sh | sh
```

```powershell tab=Windows
irm https://github.com/Twilight-Techy/storine/releases/latest/download/install.ps1 | iex
```

The script:

1. finds the latest release (or the one in `STORINE_VERSION`);
2. downloads the archive for your system and CPU;
3. checks it against the release's `SHA256SUMS`, and stops if it does not match;
4. copies `storage-server`, `storage-cli`, `storine-sql` and `storine-console` into the install folder.

| System | Default folder | PATH |
| :--- | :--- | :--- |
| Linux, macOS | `~/.local/bin`, or `/usr/local/bin` when run as root | The script tells you the line to add if the folder is not on your PATH |
| Windows | `%LOCALAPPDATA%\Programs\Storine\bin` | Added to your user PATH. Open a new terminal to use it |

Two environment variables change what it does:

| Variable | Meaning |
| :--- | :--- |
| `STORINE_VERSION` | Version to install, such as `0.2.0` or `v0.2.0`. Default: the latest release |
| `STORINE_INSTALL_DIR` | Folder to install into |

```bash tab=Linux and macOS
curl -fsSL https://github.com/Twilight-Techy/storine/releases/latest/download/install.sh | STORINE_VERSION=0.2.0 STORINE_INSTALL_DIR=/opt/storine/bin sh
```

```powershell tab=Windows
$env:STORINE_VERSION = "0.2.0"; $env:STORINE_INSTALL_DIR = "C:\storine\bin"
irm https://github.com/Twilight-Techy/storine/releases/latest/download/install.ps1 | iex
```

The Linux and macOS script needs `curl`, `tar` and `sha256sum` or `shasum`. On Linux it refuses musl systems such as Alpine, since the binaries link glibc. The Windows script runs in Windows PowerShell 5.1 and in PowerShell 7.

Check the install:

```bash
storage-server --version
```

## From a release archive

Download the archive for your system from the [latest release](https://github.com/Twilight-Techy/storine/releases/latest):

| System | Archive |
| :--- | :--- |
| Linux x86_64 (glibc 2.28 or newer) | `storine-<version>-x86_64-linux.tar.gz` |
| Linux arm64 (glibc 2.28 or newer) | `storine-<version>-aarch64-linux.tar.gz` |
| macOS 11 or newer, Apple silicon | `storine-<version>-aarch64-macos.tar.gz` |
| macOS 11 or newer, Intel | `storine-<version>-x86_64-macos.tar.gz` |
| Windows 10 or newer, x86_64 | `storine-<version>-x86_64-windows.zip` |

Each archive holds a `bin/` folder with the four programs, the licences, the README and the changelog. Check the download against `SHA256SUMS` from the same release, then unpack it and put `bin/` on your PATH:

```bash
sha256sum --check --ignore-missing SHA256SUMS     # macOS: shasum -a 256 --check --ignore-missing SHA256SUMS
tar -xzf storine-0.2.0-x86_64-linux.tar.gz
./storine-0.2.0-x86_64-linux/bin/storage-server --version
```

The binaries are built for each architecture's baseline CPU. At run time they pick hardware CRC32C, AES and AVX2 with FMA when the CPU has them, so a release build measured as fast as a build for the exact CPU (section 8.6 of the [benchmark report](../../docs/BENCHMARK_REPORT.md)).

Windows on ARM runs the x86_64 build under emulation.

## From source

You need Rust 1.82 or newer. The workspace has no third-party dependencies, so the build needs no network access beyond fetching the repository.

```bash
git clone https://github.com/Twilight-Techy/storine
cd storine
cargo build --release
ls target/release/{storage-server,storage-cli,storine-sql,storine-console}
```

A source build targets the machine it runs on. To build the same portable archives as a release, run `tools/release/build-dist.sh <version> <out-dir>`.

## Upgrading and uninstalling

To upgrade, run the install script again. It replaces the programs in place, and a running server keeps the old binary until it restarts. Before moving a server or cluster to a new **minor** version (0.1 to 0.2), read [upgrades and compatibility](../../docs/COMPATIBILITY.md): before 1.0, a minor release needs every node stopped.

To uninstall, delete the four programs from the install folder. On Windows, also remove the folder from your user PATH. Storine keeps nothing else on the machine: your data lives in the data directories you created.

## Next

Start a server and connect to it: [quick start](quick-start.md).
