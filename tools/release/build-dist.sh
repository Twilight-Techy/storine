#!/usr/bin/env bash
# Builds the release binaries for this machine's OS and architecture, checks them, smoke
# tests them and packages them. The release workflow (.github/workflows/release.yml) runs it
# on GitHub's runners:
#
#   Linux    inside `rockylinux:8` (glibc 2.28), so the binaries run on any Linux with glibc
#            2.28 or newer: RHEL/Rocky/Alma 8+, Debian 10+, Ubuntu 18.10+, Amazon Linux 2023.
#              docker run --rm -v "$PWD:/src" -w /src rockylinux:8 bash tools/release/build-dist.sh 0.1.0 dist
#   macOS    on the runner, for macOS 11 or newer.
#   Windows  in Git Bash on the runner, for Windows 10 / Server 2016 or newer (MSVC runtime
#            linked statically).
#
# The CPU target is the architecture's baseline, not `native` (`.cargo/config.toml`, for
# local builds): the hot paths pick hardware CRC32C, AES and AVX2 at run time either way,
# and docs/BENCHMARK_REPORT.md section 8.6 measured what the baseline costs.
set -euo pipefail
version=${1:?version} out=${2:?out dir}
src=$(pwd)
toolchain=${RUST_TOOLCHAIN:-1.99.1}

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=macos ;;
  MINGW* | MSYS* | CYGWIN*) os=windows ;;
  *) echo "unsupported OS $(uname -s)" >&2; exit 1 ;;
esac
arch=$(uname -m)
case "$arch" in
  x86_64 | amd64) arch=x86_64 cpu=x86-64 ;;
  aarch64 | arm64) arch=aarch64 cpu=generic ;;
  *) echo "unsupported architecture $arch" >&2; exit 1 ;;
esac
# macOS: Rust's own baselines for its Apple targets (every Intel Mac it supports has SSSE3).
[ "$os/$arch" = macos/aarch64 ] && cpu=apple-m1
[ "$os/$arch" = macos/x86_64 ] && cpu=core2
exe=""
[ "$os" = windows ] && exe=.exe
name="storine-$version-$arch-$os"

if [ "$os" = linux ] && { ! command -v gcc > /dev/null || ! command -v objdump > /dev/null; }; then
  dnf -y -q install gcc binutils tar gzip > /dev/null
fi
if ! command -v cargo > /dev/null; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y -q --profile minimal --default-toolchain "$toolchain"
  # shellcheck source=/dev/null
  . "$HOME/.cargo/env"
fi
rustc --version

export RUSTFLAGS="-C target-cpu=$cpu"
[ "$os" = windows ] && RUSTFLAGS="$RUSTFLAGS -C target-feature=+crt-static"
export MACOSX_DEPLOYMENT_TARGET=11.0
export CARGO_TARGET_DIR="$src/target-dist"
cargo build --release --locked -p storage-server -p storage-cli -p storage-sql -p storage-console --bins
bins=(storage-server storage-cli storine-sql storine-console)
bin="$CARGO_TARGET_DIR/release"

size_of() { wc -c < "$1" | tr -d ' '; }
sha256() { if command -v sha256sum > /dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi; }

echo "== checks"
fail=0
for b in "${bins[@]}"; do
  f="$bin/$b$exe"
  echo "$b$exe: $(size_of "$f") bytes"
  case "$os" in
    linux)
      # RULE-MEM-03 ceilings (docs/RULES.md) are set for Linux x86_64.
      declare -A ceiling=([storage-server]=5767168 [storage-cli]=1835008 [storine-sql]=8388608 [storine-console]=1048576)
      if [ "$arch" = x86_64 ] && [ "$(size_of "$f")" -ge "${ceiling[$b]}" ]; then
        echo "$b is over its ${ceiling[$b]}-byte ceiling"; fail=1
      fi
      glibc=$(objdump -T "$f" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1)
      libs=$(objdump -p "$f" | awk '/NEEDED/ {print $2}' | tr '\n' ' ')
      echo "  needs $glibc, links $libs"
      [ "$(printf '%s\nGLIBC_2.28\n' "$glibc" | sort -V | tail -1)" = GLIBC_2.28 ] || { echo "$b needs $glibc, newer than 2.28"; fail=1; }
      for l in $libs; do
        case "$l" in libc.so.* | libm.so.* | libgcc_s.so.* | libpthread.so.* | libdl.so.* | librt.so.* | ld-linux*) ;;
          *) echo "$b links $l, beyond libc"; fail=1 ;;
        esac
      done
      ;;
    macos)
      libs=$(otool -L "$f" | tail -n +2 | awk '{print $1}' | tr '\n' ' ')
      minos=$(otool -l "$f" | awk '/minos/ {print $2; exit}')
      echo "  minimum macOS $minos, links $libs"
      for l in $libs; do
        case "$l" in /usr/lib/libSystem.B.dylib | /usr/lib/libiconv.2.dylib | /usr/lib/libresolv.9.dylib) ;;
          *) echo "$b links $l, beyond the system libraries"; fail=1 ;;
        esac
      done
      ;;
  esac
done
[ "$fail" = 0 ]

echo "== smoke test"
tmp=$(mktemp -d)
trap 'kill "${server:-}" 2> /dev/null || true; rm -rf "$tmp"' EXIT
"$bin/storage-server$exe" gen-ca "$tmp/ca" > /dev/null
"$bin/storage-server$exe" gen-cert "$tmp/ca" "$tmp/node" localhost > /dev/null
"$bin/storine-sql$exe" --help > /dev/null
"$bin/storine-console$exe" --help | grep -q "web console"
{ "$bin/storage-cli$exe" 2>&1 || true; } | grep -q remote-health
port=$((20000 + RANDOM % 20000))
"$bin/storage-server$exe" "$tmp/data" "127.0.0.1:$port" > "$tmp/server.log" 2>&1 &
server=$!
for _ in $(seq 1 100); do
  grep -q listening "$tmp/server.log" && break
  sleep 0.1
done
"$bin/storage-cli$exe" remote-health "127.0.0.1:$port"
cport=$((port + 1))
STORINE_CONSOLE_KEY=smoke-test-key-0123 "$bin/storine-console$exe" --listen "127.0.0.1:$cport" "storine://127.0.0.1:$port" > "$tmp/console.log" 2>&1 &
console=$!
for _ in $(seq 1 100); do
  grep -q "open http" "$tmp/console.log" && break
  sleep 0.1
done
grep -q "connected to storine://127.0.0.1:$port" "$tmp/console.log"
kill "$console"
wait "$console" 2> /dev/null || true
kill "$server"
wait "$server" 2> /dev/null || true

echo "== package"
mkdir -p "$out/$name/bin"
for b in "${bins[@]}"; do cp "$bin/$b$exe" "$out/$name/bin/"; done
cp LICENSE LICENSE-APACHE README.md CHANGELOG.md "$out/$name/"
if [ "$os" = windows ]; then
  archive="$name.zip"
  (cd "$out" && 7z a -tzip -bso0 "$archive" "$name")
else
  archive="$name.tar.gz"
  tar -C "$out" -czf "$out/$archive" "$name"
fi
rm -rf "${out:?}/$name"
(cd "$out" && sha256 "$archive" > "$archive.sha256")
cat "$out/$archive.sha256"
