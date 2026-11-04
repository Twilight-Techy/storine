"""Seed sweeps and test suites on Modal (`RULE-TEST-03` at scale).

The deterministic simulators replay one seed per run (`STORINE_SIM_SEED=s`). This runs every
seed of every seeded test in parallel containers instead of serially on one machine:

1. the working tree (tracked and new files, uncommitted changes included; SDKs other than the
   Rust one and the SQL client fixtures left out) is uploaded as a tarball;
2. one large container builds the test binaries in release mode, with `target/` cached on a
   Modal volume so later builds are incremental;
3. seeds are split into shards across many small containers; every failing seed is reported
   with its replay command.

Usage (from the repository root):

    modal run tools/modal/sweep.py --tests txn_simulation,multi_range_simulation --seeds 300
    modal run tools/modal/sweep.py --tests txn_simulation --only bank_transfers_with_meta --seeds 1000
    modal run tools/modal/sweep.py --suite-args "-p storage-core -p storage-index"   # a whole `cargo test`
    modal run tools/modal/sweep.py --suite-args "-p storage-server --test process_chaos -- --nocapture" --env STORINE_CHAOS_SECS=90
    modal run tools/modal/sweep.py --suite-args "-p storage-server --test process_chaos" --repeat 12  # hunt a rare failure
    modal run tools/modal/sweep.py --tools all           # BI and GUI tools against the SQL gateway
    modal run tools/modal/sweep.py --sdks all            # all 8 SDK suites against range clusters
    modal run tools/modal/sweep.py --sdks python --plain # against a plain server

Tests whose body calls `sweep(` are seeded: seeds 1..=N in shards (`STORINE_SIM_SEED_RANGE`),
plus one run at their default seeds for their aggregate checks; the others run once.
"""

import io
import json
import os
import re
import subprocess
import tarfile
import time

import modal

RUST = "1.99.1"
app = modal.App("storine-sweeps")
target_vol = modal.Volume.from_name("storine-target", create_if_missing=True)
image = (
    modal.Image.debian_slim()
    .apt_install("build-essential", "curl", "ca-certificates", "git")
    .run_commands(f"curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain {RUST}")
    .env({"PATH": "/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "CARGO_TARGET_DIR": "/target/t", "CARGO_INCREMENTAL": "0",
          # Builders and runners are different CPUs: never `target-cpu=native` (the repo's
          # default) here. x86-64-v3 keeps the AVX2 / BMI2 / SSE4.2 paths under test.
          "RUSTFLAGS": "-C target-cpu=x86-64-v3"})
)

# Every SDK's toolchain, for their end-to-end suites.
sdk_image = (
    image.apt_install("python3", "nodejs", "npm", "default-jdk-headless", "maven", "php-cli", "libicu-dev", "libssl-dev", "libyaml-dev", "zlib1g-dev", "libffi-dev", "autoconf", "bison", "patch")
    .run_commands(
        # The Ruby SDK needs 3.2+ (Debian packages 3.1).
        "git clone --depth 1 https://github.com/rbenv/ruby-build /tmp/ruby-build && /tmp/ruby-build/bin/ruby-build 3.3.6 /usr/local",
        "curl -sSfL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz | tar -C /usr/local -xz",
        "curl -sSfL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 8.0 --install-dir /usr/local/dotnet",
    )
    .env({"PATH": "/usr/local/go/bin:/usr/local/dotnet:/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "DOTNET_ROOT": "/usr/local/dotnet", "DOTNET_CLI_TELEMETRY_OPTOUT": "1"})
)

SDK_SUITES = {
    "python": ("sdks/python", "python3 -m unittest tests.test_vectors tests.test_e2e"),
    "typescript": ("sdks/typescript", "npm install --no-audit --no-fund && npm test"),
    "go": ("sdks/go", "go test ."),
    "java": ("sdks/java", "mvn -q -B test"),
    "csharp": ("sdks/csharp/tests/Storine.Tests", "dotnet test --nologo"),
    "rust": ("sdks/rust", "cargo test --release"),
    "php": ("sdks/php", "php tests/harness.php"),
    "ruby": ("sdks/ruby", "ruby -Ilib test/test_vectors.rb && ruby -Ilib test/test_e2e.rb"),
}

EXCLUDE = (
    re.compile(r"^crates/storage-sql/tests/clients/"),
    re.compile(r"^context/"),
)
# Only the Rust SDK is needed to build the workspace.
OTHER_SDKS = re.compile(r"^sdks/(?!rust/)")


def source_tarball(root: str, all_sdks: bool = False, clients: bool = False) -> bytes:
    """Tracked and untracked-but-not-ignored files, with their mtimes (cargo's fingerprints)."""
    out = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root, capture_output=True, check=True).stdout
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path in sorted(set(p for p in out.decode().split("\0") if p)):
            full = os.path.join(root, path)
            if (not clients and any(e.match(path) for e in EXCLUDE)) or not os.path.isfile(full) or (not all_sdks and OTHER_SDKS.match(path)):
                continue
            tar.add(full, arcname=path)
    return buf.getvalue()


def unpack(src: bytes) -> None:
    with tarfile.open(fileobj=io.BytesIO(src), mode="r:gz") as tar:
        tar.extractall("/src", filter="data")


@app.function(image=image, cpu=16, memory=32768, timeout=3600, volumes={"/target": target_vol})
def build(src: bytes, tests: list[str]) -> dict[str, bytes]:
    """Builds the named integration-test binaries of storage-core; returns name -> executable."""
    unpack(src)
    args = ["cargo", "test", "--release", "-p", "storage-core", "--no-run", "--message-format=json"]
    for t in tests:
        args += ["--test", t]
    t0 = time.time()
    proc = subprocess.run(args, cwd="/src", capture_output=True, text=True)
    target_vol.commit()
    if proc.returncode != 0:
        raise RuntimeError("build failed:\n" + proc.stderr[-6000:])
    bins = {}
    for line in proc.stdout.splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("executable") and msg["target"]["name"] in tests:
            with open(msg["executable"], "rb") as f:
                bins[msg["target"]["name"]] = f.read()
    print(f"built {sorted(bins)} in {time.time() - t0:.0f}s")
    return bins


@app.function(image=image, cpu=2, memory=4096, timeout=3600, max_containers=200, retries=2)
def run_shard(binary: bytes, name: str, test: str, lo: int, hi: int) -> list[dict]:
    """Runs `test` over seeds `lo..=hi` in one process (`lo == 0`: once, at its default seeds,
    which also enforces the test's aggregate checks). A seeded shard stops at its first
    failing seed, so after one it resumes past it. Only per-seed failures (with a `replay:`
    line) count in a shard: aggregate checks ("some seed moved a replica") need the whole
    sweep and are enforced by the default run."""
    import tempfile

    fd, path = tempfile.mkstemp(prefix=f"{name}-", dir="/tmp")
    with os.fdopen(fd, "wb") as f:
        f.write(binary)
    os.chmod(path, 0o755)
    failures = []
    while True:
        env = dict(os.environ)
        if lo:
            env["STORINE_SIM_SEED_RANGE"] = f"{lo}-{hi}"
        t0 = time.time()
        proc = subprocess.run([path, test, "--exact", "--test-threads=1"], env=env, capture_output=True, text=True, timeout=3600)
        if proc.returncode == 0:
            return failures
        out = proc.stdout + proc.stderr
        replay = re.search(r"STORINE_SIM_SEED=(\d+)", out)
        msg = [l for l in out.splitlines() if "panicked" in l or " seed " in l or "assertion" in l] or out.strip().splitlines()[-8:] or [f"exit status {proc.returncode}"]
        msg.append(f"(exit status {proc.returncode})")
        if not lo or not replay:
            if lo:
                return failures  # an aggregate check of the shard alone: not a failure
            failures.append({"test": f"{name}::{test}", "seed": 0, "secs": round(time.time() - t0, 1), "message": "\n".join(msg[:6])[:2000]})
            return failures
        seed = int(replay.group(1))
        failures.append({"test": f"{name}::{test}", "seed": seed, "secs": round(time.time() - t0, 1), "message": "\n".join(msg[:6])[:2000]})
        if seed >= hi:
            return failures
        lo = seed + 1


@app.function(image=image, cpu=16, memory=32768, timeout=14400, volumes={"/target": target_vol})
def suite(src: bytes, cargo_args: str, env: dict[str, str], repeat: int = 1) -> tuple[int, str]:
    """Runs `cargo test --release <cargo_args>` with `env` up to `repeat` times, stopping at the
    first failure; returns (exit code, summary, with the end of the output on a failure)."""
    unpack(src)
    summary = []
    for attempt in range(1, repeat + 1):
        proc = subprocess.run(f"cargo test --release --no-fail-fast {cargo_args} 2>&1", shell=True, cwd="/src", capture_output=True, text=True, env={**os.environ, **env})
        target_vol.commit()
        lines = [l for l in proc.stdout.splitlines() if l.startswith("test result") or "FAILED" in l or "panicked" in l or l.startswith("error") or "process chaos" in l or "range cluster" in l or l.startswith("bench:") or l.startswith("soak")]
        summary.append(f"--- run {attempt}/{repeat}: exit {proc.returncode}")
        summary.extend(lines[-200:])
        if proc.returncode != 0:
            if repeat > 1:
                summary.append(proc.stdout[-60000:])
            return proc.returncode, "\n".join(summary)
    return 0, "\n".join(summary)


@app.function(image=image, cpu=16, memory=32768, timeout=3600, volumes={"/target": target_vol})
def build_server(src: bytes) -> bytes:
    """Builds `storage-server` (release); returns the executable."""
    unpack(src)
    proc = subprocess.run("cargo build --release -p storage-server 2>&1", shell=True, cwd="/src", capture_output=True, text=True)
    target_vol.commit()
    if proc.returncode != 0:
        raise RuntimeError("build failed:\n" + proc.stdout[-6000:])
    with open("/target/t/release/storage-server", "rb") as f:
        return f.read()


# Real clients of the SQL gateway: tool suites (`TASK-150`).
tools_image = image.apt_install("postgresql-client", "default-jdk-headless", "unixodbc", "unixodbc-dev", "odbc-postgresql", "python3", "python3-pip", "libpq-dev").run_commands(
    "python3 -m pip install --break-system-packages --quiet sqlalchemy psycopg2-binary 'psycopg[binary]' pymysql pyodbc"
)


@app.function(image=image, cpu=16, memory=32768, timeout=3600, volumes={"/target": target_vol})
def build_sql(src: bytes) -> bytes:
    """Builds `storine-sql` (release); returns the executable."""
    unpack(src)
    proc = subprocess.run("cargo build --release -p storage-sql 2>&1", shell=True, cwd="/src", capture_output=True, text=True)
    target_vol.commit()
    if proc.returncode != 0:
        raise RuntimeError("build failed:\n" + proc.stdout[-6000:])
    with open("/target/t/release/storine-sql", "rb") as f:
        return f.read()


@app.function(image=tools_image, cpu=8, memory=16384, timeout=2400)
def run_tools(src: bytes, gateway: bytes, only: str) -> tuple[int, str]:
    """Runs the BI and GUI tool suites against a gateway; returns (exit code, their output)."""
    unpack(src)
    with open("/tmp/storine-sql", "wb") as f:
        f.write(gateway)
    os.chmod("/tmp/storine-sql", 0o755)
    env = {**os.environ, "STORINE_SQL_BIN": "/tmp/storine-sql"}
    proc = subprocess.run(f"bash crates/storage-sql/tests/clients/tools/run.sh {only} 2>&1", shell=True, cwd="/src", capture_output=True, text=True, env=env)
    return proc.returncode, proc.stdout[-60000:]


@app.function(image=sdk_image, cpu=4, memory=8192, timeout=3600, retries=1)
def run_sdk(src: bytes, server: bytes, lang: str, ranges: bool) -> tuple[str, int, str]:
    """Runs one SDK's conformance and end-to-end suite against `server` (range mode if asked)."""
    unpack(src)
    with open("/tmp/storage-server", "wb") as f:
        f.write(server)
    os.chmod("/tmp/storage-server", 0o755)
    env = {**os.environ, "CARGO_TARGET_DIR": "/tmp/sdk-target"}
    if ranges:
        env |= {"STORINE_SERVER": "/src/tools/range-server.sh", "STORINE_RANGE_BIN": "/tmp/storage-server", "STORINE_RANGES": "1"}
        os.chmod("/src/tools/range-server.sh", 0o755)
    else:
        env["STORINE_SERVER"] = "/tmp/storage-server"
    cwd, cmd = SDK_SUITES[lang]
    t0 = time.time()
    try:
        proc = subprocess.run(cmd, shell=True, cwd=f"/src/{cwd}", env=env, capture_output=True, text=True, timeout=3000)
        code, out = proc.returncode, proc.stdout + proc.stderr
    except subprocess.TimeoutExpired as e:
        code, out = 124, f"timed out\n{e.stdout}"
    return lang, code, f"[{time.time() - t0:.0f}s]\n" + out[-6000:]


def seeded_tests(root: str, name: str) -> tuple[list[str], list[str]]:
    """(seeded, single-run) test functions of `crates/storage-core/tests/<name>.rs`."""
    text = open(os.path.join(root, "crates/storage-core/tests", f"{name}.rs")).read()
    seeded, single = [], []
    for m in re.finditer(r"#\[test\]\s*(?:#\[[^\]]*\]\s*)*fn (\w+)\(\)", text):
        start = m.end()
        nxt = text.find("#[test]", start)
        body = text[start: nxt if nxt != -1 else len(text)]
        (seeded if "sweep(" in body else single).append(m.group(1))
    return seeded, single


@app.local_entrypoint()
def main(tests: str = "txn_simulation,multi_range_simulation", seeds: int = 300, only: str = "", shard: int = 10, suite_args: str = "", env: str = "", sdks: str = "", plain: bool = False, repeat: int = 1, tools: str = ""):
    root = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True).stdout.strip()
    if tools:
        # BI and GUI tools against the gateway; `all`, or suite functions of test_tools.py.
        src = source_tarball(root, all_sdks=True, clients=True)
        code, out = run_tools.remote(src, build_sql.remote(src), "" if tools == "all" else tools.replace(",", " "))
        print(out)
        print(f"tool suites exit code {code}")
        if code:
            raise SystemExit(1)
        return
    if sdks:
        # SDK end-to-end suites, against range clusters unless --plain.
        src = source_tarball(root, all_sdks=True)
        server = build_server.remote(src)
        langs = list(SDK_SUITES) if sdks == "all" else sdks.split(",")
        failed = []
        for lang, code, out in run_sdk.starmap([(src, server, l, not plain) for l in langs]):
            print(f"=== {lang}: {'ok' if code == 0 else f'FAILED ({code})'}")
            if code != 0:
                failed.append(lang)
                print(out)
        print(f"SDK SUITES: {len(langs) - len(failed)}/{len(langs)} passed; failed: {failed}")
        if failed:
            raise SystemExit(1)
        return
    src = source_tarball(root)
    print(f"source: {len(src) / 1e6:.1f} MB")
    if suite_args:
        extra = dict(kv.split("=", 1) for kv in env.split(",") if "=" in kv)
        code, summary = suite.remote(src, suite_args, extra, repeat)
        print(summary)
        print(f"cargo test exit code {code}")
        if code:
            raise SystemExit(1)
        return
    names = [t for t in tests.split(",") if t]
    bins = build.remote(src, names)
    jobs = []
    for name in names:
        seeded, single = seeded_tests(root, name)
        for t in seeded:
            if only and t not in only.split(","):
                continue
            for lo in range(1, seeds + 1, shard):
                jobs.append((bins[name], name, t, lo, min(lo + shard - 1, seeds)))
            jobs.append((bins[name], name, t, 0, 0))
        for t in single:
            if not only or t in only.split(","):
                jobs.append((bins[name], name, t, 0, 0))
    print(f"{len(jobs)} shards")
    t0 = time.time()
    failures = [f for shard_failures in run_shard.starmap(jobs) for f in shard_failures]
    print(f"done in {time.time() - t0:.0f}s")
    if not failures:
        print(f"ALL PASSED: {', '.join(names)} at {seeds} seeds")
        return
    failures.sort(key=lambda f: (f["test"], f["seed"]))
    for f in failures:
        what = f"seed {f['seed']}" if f["seed"] else "default run"
        env = f"STORINE_SIM_SEED={f['seed']} " if f["seed"] else ""
        print(f"FAILED {f['test']} {what} ({f['secs']}s)\n  {f['message']}\n  replay: {env}cargo test --release -p storage-core --test {f['test'].split('::')[0]} {f['test'].split('::')[1]}")
    raise SystemExit(1)
