#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$ROOT_DIR/bench/results/ycsb_a_mysql_versions"
BASE_YCSB_XML="$ROOT_DIR/bench/config/ycsb.xml"
BENCHBASE_JAR="$ROOT_DIR/third_party/benchbase/benchbase-mysql/benchbase.jar"
BENCHBASE_HOME="$(dirname "$BENCHBASE_JAR")"
BENCHBASE_RESULTS_DIR_PRIMARY="$BENCHBASE_HOME/results"
BENCHBASE_RESULTS_DIR_LEGACY="$ROOT_DIR/third_party/benchbase/results"

TERMINALS="${TERMINALS:-8}"
DURATION="${DURATION:-20}"

TARGET_NAMES=("mysql-5.7" "mysql-8.0.43")
TARGET_IMAGES=("docker.io/library/mysql:5.7" "docker.io/library/mysql:8.0.43")
TARGET_PORTS=("34057" "34080")

mkdir -p "$OUT_DIR" "$BENCHBASE_RESULTS_DIR_PRIMARY" "$BENCHBASE_RESULTS_DIR_LEGACY"

log() { echo "[$(date +'%H:%M:%S')] $*" >&2; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: command not found: $1" >&2; exit 1; }
}

require_cmd sed
require_cmd awk
require_cmd python3
require_cmd java

if [[ ! -f "$BENCHBASE_JAR" ]]; then
  echo "ERROR: BenchBase jar not found: $BENCHBASE_JAR" >&2
  echo "Build it first, e.g.:" >&2
  echo "  cd $ROOT_DIR/third_party/benchbase && ./mvnw -DskipTests -P mysql clean package && unzip -o target/benchbase-mysql.zip" >&2
  exit 1
fi

# Prefer podman. If using docker and socket permission is denied, automatically fallback to sudo docker.
RUNTIME=""
RUNCMD=()
if command -v podman >/dev/null 2>&1; then
  RUNTIME="podman"
  RUNCMD=(podman)
elif command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    RUNTIME="docker"
    RUNCMD=(docker)
  elif command -v sudo >/dev/null 2>&1 && sudo -n docker info >/dev/null 2>&1; then
    RUNTIME="docker"
    RUNCMD=(sudo docker)
  else
    echo "ERROR: docker exists but is not accessible by current user." >&2
    echo "Hint: add your user to docker group and re-login, or run with sudo." >&2
    exit 1
  fi
else
  echo "ERROR: neither podman nor docker is installed" >&2
  exit 1
fi

runtime_exec() {
  "${RUNCMD[@]}" "$@"
}

wait_mysql() {
  local port="$1"
  local timeout="${2:-120}"
  local start now
  start="$(date +%s)"
  while true; do
    if python3 - "$port" <<'PY' >/dev/null 2>&1
import sys, pymysql
port = int(sys.argv[1])
conn = pymysql.connect(host='127.0.0.1', port=port, user='root', password='', autocommit=True)
conn.close()
PY
    then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= timeout )); then
      return 1
    fi
    sleep 1
  done
}

prepare_db() {
  local port="$1"
  python3 - "$port" <<'PY'
import sys, pymysql
port = int(sys.argv[1])
conn = pymysql.connect(host='127.0.0.1', port=port, user='root', password='', autocommit=True)
cur = conn.cursor()
cur.execute('DROP DATABASE IF EXISTS benchbase')
cur.execute('CREATE DATABASE benchbase')
conn.close()
PY
}

create_ycsb_a_config() {
  local port="$1"
  local terminals="$2"
  local duration="$3"
  local out="$4"

  cp "$BASE_YCSB_XML" "$out"
  sed -i -E "s|<url>jdbc:mysql://localhost:[0-9]+/benchbase\?rewriteBatchedStatements=true&amp;sslMode=DISABLED</url>|<url>jdbc:mysql://localhost:${port}/benchbase?rewriteBatchedStatements=true\&amp;sslMode=DISABLED</url>|" "$out"
  sed -i -E "s|<terminals>[0-9]+</terminals>|<terminals>${terminals}</terminals>|" "$out"
  sed -i -E "s|<time>[0-9]+</time>|<time>${duration}</time>|" "$out"
  sed -i -E "s|<weights>[^<]+</weights>|<weights>50,0,0,50,0,0</weights>|" "$out"
}

throughput_from_latest_csv() {
  local run_start_epoch="$1"
  python3 - "$BENCHBASE_RESULTS_DIR_PRIMARY" "$BENCHBASE_RESULTS_DIR_LEGACY" "$run_start_epoch" <<'PY'
import csv, sys
from pathlib import Path

search_dirs = [Path(p) for p in sys.argv[1:3] if p]
run_start_epoch = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
candidates = []
for d in search_dirs:
    if d.exists():
        candidates.extend(d.glob('*.csv'))

# Only consider files generated/updated by current benchmark run.
csvs = [p for p in set(candidates) if p.stat().st_mtime >= run_start_epoch - 1]
csvs = sorted(csvs, key=lambda p: p.stat().st_mtime, reverse=True)
if not csvs:
    raise SystemExit(2)

key_aliases = (
    'throughput(req/sec)',
    'throughput(req/s)',
    'throughput',
    'requests/sec',
    'req/s',
    'throughput (requests/second)',
    'throughput(req_per_sec)',
)

for path in csvs:
    try:
        with path.open(newline='') as f:
            r = csv.DictReader(f)
            if not r.fieldnames:
                continue
            lookup = {k.strip().lower(): k for k in r.fieldnames}
            cand = None
            for key in key_aliases:
                if key in lookup:
                    cand = lookup[key]
                    break
            if cand is None:
                continue

            rows = list(r)
            if not rows:
                continue

            vals = []
            for row in rows:
                t = (row.get('Transaction Type') or row.get('transaction type') or '').strip().lower()
                v = (row.get(cand) or '').strip()
                if t == 'total' and v:
                    vals.append(float(v))
            if vals:
                print(max(vals))
                raise SystemExit(0)

            for row in rows:
                v = (row.get(cand) or '').strip()
                if v:
                    vals.append(float(v))
            if vals:
                print(max(vals))
                raise SystemExit(0)
    except Exception:
        continue
raise SystemExit(3)
PY
}

bench_target() {
  local name="$1" image="$2" port="$3"
  local container="bench-${name}"
  local cfg
  cfg="$(mktemp /tmp/ycsb-a-XXXXXX.xml)"

  runtime_exec rm -f "$container" >/dev/null 2>&1 || true

  log "Pulling ${image}"
  if ! runtime_exec pull "$image" >/tmp/pull_${name}.log 2>&1; then
    echo "FAILED_PULL::$(tr '\n' ' ' </tmp/pull_${name}.log)"
    rm -f "$cfg"
    return 1
  fi

  log "Starting ${container} on port ${port}"
  if ! runtime_exec run -d --name "$container" -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p "${port}:3306" "$image" >/tmp/run_${name}.log 2>&1; then
    echo "FAILED_START::$(tr '\n' ' ' </tmp/run_${name}.log)"
    runtime_exec rm -f "$container" >/dev/null 2>&1 || true
    rm -f "$cfg"
    return 1
  fi

  create_ycsb_a_config "$port" "$TERMINALS" "$DURATION" "$cfg"

  if ! wait_mysql "$port" 180; then
    echo "FAILED_WAIT::MySQL did not become ready on port $port"
    runtime_exec logs "$container" >/tmp/logs_${name}.log 2>&1 || true
    runtime_exec rm -f "$container" >/dev/null 2>&1 || true
    rm -f "$cfg"
    return 1
  fi

  prepare_db "$port"

  log "Running BenchBase YCSB-A for ${name}"
  local run_start_epoch
  run_start_epoch="$(date +%s)"
  if ! (cd "$BENCHBASE_HOME" && java -jar "$BENCHBASE_JAR" -b ycsb -c "$cfg" --create=true --load=true --execute=true) >"/tmp/benchbase_${name}.log" 2>&1; then
    echo "FAILED_BENCHBASE::$(tr '\n' ' ' </tmp/benchbase_${name}.log)"
    runtime_exec rm -f "$container" >/dev/null 2>&1 || true
    rm -f "$cfg"
    return 1
  fi

  local throughput
  if ! throughput="$(throughput_from_latest_csv "$run_start_epoch")"; then
    # Fallback: parse throughput from BenchBase log output.
    throughput="$({ python3 - "/tmp/benchbase_${name}.log" <<'PY'
import re, sys
text = open(sys.argv[1], 'r', errors='ignore').read()
patterns = [
    r"Throughput\(req/sec\)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)",
    r"Throughput\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)",
    r"\bTOTAL\b.*?([0-9]+(?:\.[0-9]+)?)\s*$",
]
for pat in patterns:
    m = re.findall(pat, text, flags=re.MULTILINE)
    if m:
        print(m[-1])
        raise SystemExit(0)
raise SystemExit(1)
PY
    } || true)"

    if [[ -z "$throughput" ]]; then
      echo "FAILED_PARSE::Could not parse throughput from BenchBase CSVs/logs"
      runtime_exec rm -f "$container" >/dev/null 2>&1 || true
      rm -f "$cfg"
      return 1
    fi
  fi

  runtime_exec rm -f "$container" >/dev/null 2>&1 || true
  rm -f "$cfg"

  echo "OK::${throughput}"
  return 0
}

RESULT_CSV="$OUT_DIR/results.csv"
README_OUT="$OUT_DIR/README.md"

echo "version,throughput_req_per_sec" > "$RESULT_CSV"

errors=()
successes=0
throughputs=()

for i in "${!TARGET_NAMES[@]}"; do
  name="${TARGET_NAMES[$i]}"
  image="${TARGET_IMAGES[$i]}"
  port="${TARGET_PORTS[$i]}"

  log "Running ${name} with ${RUNTIME} (${image})"
  out="$(bench_target "$name" "$image" "$port" || true)"

  if [[ "$out" == OK::* ]]; then
    th="${out#OK::}"
    echo "${name},${th}" >> "$RESULT_CSV"
    throughputs+=("$th")
    successes=$((successes+1))
    log "${name}: ${th} req/sec"
  else
    err="${out#FAILED_*::}"
    errors+=("${name}: ${err}")
    log "${name}: FAILED"
  fi
done

{
  echo "# BenchBase YCSB-A benchmark (containerized MySQL)"
  echo
  printf "%s\n" "Runtime: \`$RUNTIME\`"
  printf "%s\n" "Docker command: \`$(printf '%q ' "${RUNCMD[@]}")\`"
  printf "%s\n" "Terminals: \`$TERMINALS\`"
  printf "%s\n" "Duration: \`${DURATION}s\`"
  echo
  echo "Targets:"
  for i in "${!TARGET_NAMES[@]}"; do
    echo "- ${TARGET_NAMES[$i]} -> \`${TARGET_IMAGES[$i]}\`"
  done

  if (( successes > 0 )); then
    echo
    echo "Successful runs:"
    tail -n +2 "$RESULT_CSV" | while IFS=, read -r n v; do
      [[ -n "$n" ]] && echo "- $n: $v req/sec"
    done
  fi

  if (( ${#errors[@]} > 0 )); then
    echo
    echo "Failures:"
    for e in "${errors[@]}"; do
      echo "- $e"
    done
    echo
    echo "If you saw docker socket permission errors, either:"
    echo "- run with sudo (or ensure script auto-detects sudo docker), or"
    echo "- add your user to docker group: sudo usermod -aG docker \$USER && re-login."
  fi
} > "$README_OUT"

if (( successes == 2 )); then
  python3 - "$RESULT_CSV" "$OUT_DIR/plot.png" <<'PY'
import csv, sys
import matplotlib.pyplot as plt
csv_path, out_png = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(csv_path)))
names = [r['version'] for r in rows]
vals = [float(r['throughput_req_per_sec']) for r in rows]
plt.figure(figsize=(6,4))
b = plt.bar(names, vals, color=['#4e79a7','#f28e2b'])
plt.title('BenchBase YCSB-A on InnoDB')
plt.ylabel('Throughput (req/sec)')
for bar, v in zip(b, vals):
    plt.text(bar.get_x()+bar.get_width()/2, v, f'{v:.2f}', ha='center', va='bottom', fontsize=9)
plt.tight_layout()
plt.savefig(out_png, dpi=160)
PY
  log "Saved $RESULT_CSV and $OUT_DIR/plot.png"
else
  log "Did not produce plot because both target benchmarks were not successful."
fi
