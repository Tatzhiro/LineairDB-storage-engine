#!/usr/bin/env bash
set -euo pipefail

# Find repo root robustly:
# 1) Prefer git root if available
# 2) Fallback: two levels up from this script (bench/bin -> repo root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
  :
else
  REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi

TARGET_REL="LineairDB-storage-engine/third_party/benchbase/src/main/java/com/oltpbenchmark/api/Worker.java"

# If your actual path does NOT have the extra "LineairDB-storage-engine/" prefix,
# this fallback will handle it automatically.
TARGET1="$REPO_ROOT/$TARGET_REL"
TARGET2="$REPO_ROOT/third_party/benchbase/src/main/java/com/oltpbenchmark/api/Worker.java"

if [[ -f "$TARGET1" ]]; then
  TARGET="$TARGET1"
  TARGET_REL_USED="$TARGET_REL"
elif [[ -f "$TARGET2" ]]; then
  TARGET="$TARGET2"
  TARGET_REL_USED="third_party/benchbase/src/main/java/com/oltpbenchmark/api/Worker.java"
else
  echo "ERROR: target file not found. Tried:"
  echo "  $TARGET1"
  echo "  $TARGET2"
  echo
  echo "Repo root guessed as: $REPO_ROOT"
  exit 1
fi

ts="$(date +%Y%m%d_%H%M%S)"
cp -a "$TARGET" "${TARGET}.bak.${ts}"
echo "Backup created: ${TARGET}.bak.${ts}"

python3 - "$TARGET" <<'PY'
import sys
from pathlib import Path

target = Path(sys.argv[1])
text = target.read_text(encoding="utf-8")

if 'lower.contains("got error 149") || lower.contains("deadlock")' in text:
    # Already patched. Keep script idempotent.
    print("Already patched; no method changes needed.")
else:
    signature = "    private boolean isRetryable(SQLException ex) {"
    start = text.find(signature)
    if start < 0:
        print("ERROR: isRetryable(SQLException ex) not found.", file=sys.stderr)
        sys.exit(1)

    i = start
    depth = 0
    entered = False
    end = -1
    while i < len(text):
        c = text[i]
        if c == "{":
            depth += 1
            entered = True
        elif c == "}":
            depth -= 1
            if entered and depth == 0:
                end = i + 1
                break
        i += 1

    if end < 0:
        print("ERROR: could not parse method body boundaries.", file=sys.stderr)
        sys.exit(1)

    replacement = """    private boolean isRetryable(SQLException ex) {

        String sqlState = ex.getSQLState();
        int errorCode = ex.getErrorCode();
        String message = ex.getMessage();

        LOG.debug("sql state [{}] and error code [{}]", sqlState, errorCode);

        if (sqlState == null) {
            return false;
        }

        // ------------------
        // MYSQL: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
        // ------------------
        if (errorCode == 1213 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_DEADLOCK
            return true;
        } else if (errorCode == 1205 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_WAIT_TIMEOUT
            return true;
        } else if (errorCode == 1180 && sqlState.equals("HY000")) {
            // LineairDB may surface deadlocks at COMMIT as "Got error 149"
            if (message != null) {
                String lower = message.toLowerCase(Locale.ROOT);
                return lower.contains("got error 149") || lower.contains("deadlock");
            }
        }

        // ------------------
        // POSTGRES: https://www.postgresql.org/docs/current/errcodes-appendix.html
        // ------------------
        // Postgres serialization_failure
        return errorCode == 0 && sqlState.equals("40001");
    }"""

    text = text[:start] + replacement + text[end:]

if "import java.util.Locale;" not in text:
    anchor = "import java.util.HashMap;\n"
    if anchor in text:
        text = text.replace(anchor, anchor + "import java.util.Locale;\n", 1)
    else:
        fallback = "import java.sql.Statement;\n"
        if fallback in text:
            text = text.replace(fallback, fallback + "import java.util.Locale;\n", 1)
        else:
            print("ERROR: import insertion anchor not found.", file=sys.stderr)
            sys.exit(1)

target.write_text(text, encoding="utf-8")
print("Patch applied successfully.")
PY

echo "Patched: $TARGET_REL_USED"

BENCHBASE_DIR="$REPO_ROOT/third_party/benchbase"
echo "Rebuilding BenchBase (mysql profile)..."
(
  cd "$BENCHBASE_DIR"
  ./mvnw -DskipTests -P mysql clean package
  unzip -o target/benchbase-mysql.zip
)
echo "BenchBase rebuilt and mysql zip expanded."
