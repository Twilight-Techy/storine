#!/bin/sh
# Installs the Storine binaries (storage-server, storage-cli, storine-sql, storine-console)
# from a GitHub release, on Linux (glibc 2.28 or newer) and macOS 11 or newer.
#
#   curl -fsSL https://github.com/Twilight-Techy/storine/releases/latest/download/install.sh | sh
#
# Environment:
#   STORINE_VERSION      version to install, e.g. 0.2.0 (default: the latest release)
#   STORINE_INSTALL_DIR  where the binaries go (default: /usr/local/bin as root, else ~/.local/bin)
#
# The archive is checked against the release's SHA256SUMS before anything is installed.
set -eu

repo="Twilight-Techy/storine"
base="https://github.com/$repo/releases"

say() { printf '%s\n' "storine: $*"; }
die() { printf '%s\n' "storine: error: $*" >&2; exit 1; }
need() { command -v "$1" > /dev/null 2>&1 || die "this needs '$1'"; }

need curl
need tar
need uname

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=macos ;;
  *) die "unsupported system $(uname -s): use the release archives at $base" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *) die "unsupported CPU $(uname -m): build from source (see README.md)" ;;
esac
if [ "$os" = linux ]; then
  # The Linux binaries link glibc 2.28 or newer; musl systems (Alpine) cannot run them.
  if ldd --version 2>&1 | grep -qi musl; then
    die "musl libc (e.g. Alpine) is not supported: use a glibc-based image or build from source"
  fi
fi

if command -v sha256sum > /dev/null 2>&1; then
  sha256() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum > /dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  die "this needs sha256sum or shasum to check the download"
fi

version="${STORINE_VERSION:-}"
if [ -z "$version" ]; then
  # The latest release's page redirects to its tag.
  url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$base/latest") || die "cannot reach $base/latest"
  version="${url##*/}"
fi
version="${version#v}"
case "$version" in
  "" | */* | latest) die "could not find the latest version: set STORINE_VERSION" ;;
esac

if [ -n "${STORINE_INSTALL_DIR:-}" ]; then
  dir="$STORINE_INSTALL_DIR"
elif [ "$(id -u)" = 0 ]; then
  dir=/usr/local/bin
else
  dir="$HOME/.local/bin"
fi

name="storine-$version-$arch-$os"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

say "downloading Storine $version for $arch $os"
curl -fsSL -o "$tmp/$name.tar.gz" "$base/download/v$version/$name.tar.gz" || die "no $name.tar.gz in release v$version"
curl -fsSL -o "$tmp/SHA256SUMS" "$base/download/v$version/SHA256SUMS" || die "no SHA256SUMS in release v$version"

want=$(awk -v f="$name.tar.gz" '$2 == f || $2 == "*" f {print $1}' "$tmp/SHA256SUMS")
[ -n "$want" ] || die "$name.tar.gz is not listed in SHA256SUMS"
got=$(sha256 "$tmp/$name.tar.gz")
[ "$got" = "$want" ] || die "checksum mismatch for $name.tar.gz (expected $want, got $got)"

tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
mkdir -p "$dir" || die "cannot create $dir"
for b in "$tmp/$name/bin/"*; do
  # Copy then rename, so a running binary is replaced, not overwritten in place.
  cp "$b" "$dir/.$(basename "$b").new" && chmod 755 "$dir/.$(basename "$b").new" && mv -f "$dir/.$(basename "$b").new" "$dir/$(basename "$b")" \
    || die "cannot write to $dir (set STORINE_INSTALL_DIR, or run as root for /usr/local/bin)"
done

say "installed $(cd "$tmp/$name/bin" && ls | tr '\n' ' ')into $dir"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) say "add $dir to your PATH, e.g.: export PATH=\"$dir:\$PATH\"" ;;
esac
say "start a server with: storage-server ./data 127.0.0.1:7878"
