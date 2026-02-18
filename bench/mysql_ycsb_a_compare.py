import argparse
import csv
import random
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from xml.etree import ElementTree as ET

import matplotlib.pyplot as plt
import pymysql

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "bench/results/ycsb_a_mysql_versions"
OUTDIR.mkdir(parents=True, exist_ok=True)
BASE_YCSB_XML = ROOT / "bench/config/ycsb.xml"
BENCHBASE_JAR = ROOT / "third_party/benchbase/benchbase-mysql/benchbase.jar"
BENCHBASE_RESULTS_DIR = ROOT / "third_party/benchbase/results"

TARGETS = [
    {"name": "mysql-5.7", "image": "docker.io/library/mysql:5.7", "port": 34057},
    {"name": "mysql-8.0.43", "image": "docker.io/library/mysql:8.0.43", "port": 34080},
]


def run(cmd: list[str], check: bool = True, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=True, check=check, cwd=cwd)


def detect_runtime() -> str:
    if shutil.which("podman"):
        return "podman"
    if shutil.which("docker"):
        return "docker"
    raise RuntimeError("Neither podman nor docker is installed")


def wait_mysql(port: int, timeout: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(host="127.0.0.1", port=port, user="root", password="", autocommit=True)
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"MySQL on port {port} did not become ready")


def prepare_db(port: int) -> None:
    conn = pymysql.connect(host="127.0.0.1", port=port, user="root", password="", autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS benchbase")
    cur.execute("CREATE DATABASE benchbase")
    conn.close()


def create_ycsb_a_config(port: int, terminals: int, duration_sec: int) -> Path:
    tree = ET.parse(BASE_YCSB_XML)
    root = tree.getroot()

    for node in root.findall("url"):
        node.text = f"jdbc:mysql://localhost:{port}/benchbase?rewriteBatchedStatements=true&sslMode=DISABLED"
    for node in root.findall("username"):
        node.text = "root"
    for node in root.findall("password"):
        node.text = ""
    for node in root.findall("terminals"):
        node.text = str(terminals)

    for work in root.findall("works/work"):
        time_node = work.find("time")
        if time_node is not None:
            time_node.text = str(duration_sec)
        weight_node = work.find("weights")
        if weight_node is not None:
            weight_node.text = "50,0,0,50,0,0"

    tmp = tempfile.NamedTemporaryFile(prefix="ycsb-a-", suffix=".xml", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()
    tree.write(tmp_path, encoding="utf-8", xml_declaration=True)
    return tmp_path


def find_throughput_from_csvs(result_dir: Path) -> float:
    csv_files = sorted(result_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in csv_files:
        try:
            with path.open(newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                lower = {k.lower(): k for k in reader.fieldnames}
                candidate = None
                for key in [
                    "throughput(req/sec)",
                    "throughput(req/s)",
                    "throughput",
                    "requests/sec",
                    "req/s",
                ]:
                    if key in lower:
                        candidate = lower[key]
                        break
                if candidate is None:
                    continue
                rows = list(reader)
                if not rows:
                    continue
                vals = []
                for row in rows:
                    raw = row.get(candidate, "").strip()
                    if raw:
                        vals.append(float(raw))
                if vals:
                    return vals[-1]
        except Exception:
            continue
    raise RuntimeError(f"Could not parse throughput from BenchBase CSV in {result_dir}")


def run_benchbase(config_path: Path) -> float:
    if not BENCHBASE_JAR.exists():
        raise RuntimeError(
            f"BenchBase jar not found at {BENCHBASE_JAR}. Build BenchBase first (e.g., bench/bin/install_benchbase.sh)."
        )

    BENCHBASE_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    before = set(BENCHBASE_RESULTS_DIR.glob("*.csv"))

    cmd = [
        "java",
        "-jar",
        str(BENCHBASE_JAR),
        "-b",
        "ycsb",
        "-c",
        str(config_path),
        "--create=true",
        "--load=true",
        "--execute=true",
    ]
    proc = run(cmd, check=False, cwd=ROOT / "third_party/benchbase")
    if proc.returncode != 0:
        raise RuntimeError(f"BenchBase failed: {proc.stderr.strip() or proc.stdout.strip()}")

    after = set(BENCHBASE_RESULTS_DIR.glob("*.csv"))
    new_files = sorted(after - before, key=lambda p: p.stat().st_mtime, reverse=True)
    if not new_files:
        # fallback to current directory scan
        new_files = sorted(BENCHBASE_RESULTS_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return find_throughput_from_csvs(BENCHBASE_RESULTS_DIR if not new_files else new_files[0].parent)


def bench_target(runtime: str, target: dict, terminals: int, duration_sec: int) -> float:
    name = target["name"]
    image = target["image"]
    port = target["port"]
    container = f"bench-{name}"

    run([runtime, "rm", "-f", container], check=False)

    pulled = run([runtime, "pull", image], check=False)
    if pulled.returncode != 0:
        raise RuntimeError(f"Failed to pull {image}: {pulled.stderr.strip() or pulled.stdout.strip()}")

    started = run(
        [
            runtime,
            "run",
            "-d",
            "--name",
            container,
            "-e",
            "MYSQL_ALLOW_EMPTY_PASSWORD=yes",
            "-p",
            f"{port}:3306",
            image,
        ],
        check=False,
    )
    if started.returncode != 0:
        raise RuntimeError(f"Failed to start {container}: {started.stderr.strip() or started.stdout.strip()}")

    cfg = None
    try:
        wait_mysql(port)
        prepare_db(port)
        cfg = create_ycsb_a_config(port, terminals, duration_sec)
        return run_benchbase(cfg)
    finally:
        if cfg and cfg.exists():
            cfg.unlink(missing_ok=True)
        run([runtime, "rm", "-f", container], check=False)


def plot(rows: list[tuple[str, float]]) -> None:
    fig = plt.figure(figsize=(6, 4))
    names = [n for n, _ in rows]
    vals = [v for _, v in rows]
    bars = plt.bar(names, vals, color=["#4e79a7", "#f28e2b"])
    plt.title("BenchBase YCSB-A on InnoDB")
    plt.ylabel("Throughput (req/sec)")
    for b, v in zip(bars, vals):
        plt.text(b.get_x() + b.get_width() / 2, v, f"{v:.2f}", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(OUTDIR / "plot.png", dpi=160)
    plt.close(fig)


def write_outputs(runtime: str, rows: list[tuple[str, float]], errors: list[str], terminals: int, duration_sec: int) -> None:
    with (OUTDIR / "results.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["version", "throughput_req_per_sec"])
        for name, value in rows:
            w.writerow([name, value])

    with (OUTDIR / "README.md").open("w") as f:
        f.write("# BenchBase YCSB-A benchmark (containerized MySQL)\n\n")
        f.write(f"Runtime: `{runtime}`\n")
        f.write(f"Terminals: `{terminals}`\n")
        f.write(f"Duration: `{duration_sec}s`\n\n")
        f.write("Targets:\n")
        for t in TARGETS:
            f.write(f"- {t['name']} -> `{t['image']}`\n")
        f.write("\n")

        if rows:
            f.write("Successful runs:\n")
            for n, v in rows:
                f.write(f"- {n}: {v:.2f} req/sec\n")

        if errors:
            f.write("\nFailures:\n")
            for e in errors:
                f.write(f"- {e}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run BenchBase YCSB-A on MySQL 5.7 vs 8.0.43 containers")
    parser.add_argument("--terminals", type=int, default=8)
    parser.add_argument("--duration", type=int, default=20)
    args = parser.parse_args()

    runtime = detect_runtime()
    rows: list[tuple[str, float]] = []
    errors: list[str] = []

    for t in TARGETS:
        try:
            print(f"Running {t['name']} with {runtime} ({t['image']})")
            th = bench_target(runtime, t, terminals=args.terminals, duration_sec=args.duration)
            rows.append((t["name"], th))
            print(f"{t['name']}: {th:.2f} req/sec")
        except Exception as e:
            msg = f"{t['name']}: {e}"
            print(msg)
            errors.append(msg)

    write_outputs(runtime, rows, errors, terminals=args.terminals, duration_sec=args.duration)

    if len(rows) == 2:
        plot(rows)
        print(f"Saved {OUTDIR / 'results.csv'} and {OUTDIR / 'plot.png'}")
    else:
        print("Did not produce plot because both target benchmarks were not successful.")


if __name__ == "__main__":
    main()
