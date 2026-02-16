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

TARGET_REL="LineairDB-storage-engine/third_party/benchbase/src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBBenchmark.java"

# If your actual path does NOT have the extra "LineairDB-storage-engine/" prefix,
# this fallback will handle it automatically.
TARGET1="$REPO_ROOT/$TARGET_REL"
TARGET2="$REPO_ROOT/third_party/benchbase/src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBBenchmark.java"

if [[ -f "$TARGET1" ]]; then
  TARGET="$TARGET1"
  TARGET_REL_USED="$TARGET_REL"
elif [[ -f "$TARGET2" ]]; then
  TARGET="$TARGET2"
  TARGET_REL_USED="third_party/benchbase/src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBBenchmark.java"
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

cat > "$TARGET" <<'EOF'
/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class YCSBBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(YCSBBenchmark.class);

    /**
     * The length in characters of each field
     */
    protected final int fieldSize;

    public YCSBBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        int fieldSize = YCSBConstants.MAX_FIELD_SIZE;
        if (workConf.getXmlConfig() != null && workConf.getXmlConfig().containsKey("fieldSize")) {
            fieldSize = Math.min(workConf.getXmlConfig().getInt("fieldSize"), YCSBConstants.MAX_FIELD_SIZE);
        }
        this.fieldSize = fieldSize;
        if (this.fieldSize <= 0) {
            throw new RuntimeException("Invalid YCSB fieldSize '" + this.fieldSize + "'");
        }
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        // LOADING FROM THE DATABASE IMPORTANT INFORMATION
        // LIST OF USERS
        Table t = this.getCatalog().getTable("USERTABLE");
        String userCount = SQLUtil.getMaxColSQL(this.workConf.getDatabaseType(), t, "ycsb_key");
        int maxRetries = Math.max(1, this.workConf.getMaxRetries());
        int attempt = 0;
        while (attempt < maxRetries) {
            try (Connection metaConn = this.makeConnection();
                 Statement stmt = metaConn.createStatement();
                 ResultSet res = stmt.executeQuery(userCount)) {
                int init_record_count = 0;
                while (res.next()) {
                    init_record_count = res.getInt(1);
                }

                for (int i = 0; i < workConf.getTerminals(); ++i) {
                    workers.add(new YCSBWorker(this, i, init_record_count + 1));
                }
                return workers;
            } catch (SQLException e) {
                attempt++;
                if (isRetryableInitQueryError(e) && attempt < maxRetries) {
                    LOG.warn("YCSB init query failed (attempt {}/{}). Retrying. SQLState={}, errorCode={}",
                            attempt,
                            maxRetries,
                            e.getSQLState(),
                            e.getErrorCode());
                    if (!sleepBeforeRetry(attempt)) {
                        break;
                    }
                    continue;
                }
                LOG.error(e.getMessage(), e);
                break;
            }
        }
        if (workers.isEmpty()) {
            LOG.error("No YCSB workers created; initialization query failed.");
        }
        return workers;
    }

    @Override
    protected Loader<YCSBBenchmark> makeLoaderImpl() {
        return new YCSBLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return InsertRecord.class.getPackage();
    }

    private static boolean isRetryableInitQueryError(SQLException e) {
        return e.getErrorCode() == 1213 || "40001".equals(e.getSQLState());
    }

    private static boolean sleepBeforeRetry(int attempt) {
        try {
            Thread.sleep(100L * attempt);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

}
EOF

echo "Patched: $TARGET_REL_USED"

