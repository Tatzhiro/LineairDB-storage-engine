import os
import glob
import shutil
import subprocess
import sys
import time
from typing import Tuple

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(REPO_ROOT, ".."))

MYSQLD = os.path.join(REPO_ROOT, "build", "bin", "mysqld")
MY_CNF = os.path.join(REPO_ROOT, "@github_workflows", "my.cnf")

# Ensure jemalloc is preloaded before plugin load to avoid TLS errors
MYSQLD_ENV = os.environ.copy()
MYSQLD_ENV["LD_PRELOAD"] = "/lib/x86_64-linux-gnu/libjemalloc.so.2"


def run(cmd: list[str], check: bool = True, env: dict | None = None, cwd: str | None = None) -> int:
    print("$", " ".join(cmd))
    return subprocess.run(cmd, check=check, env=env, cwd=cwd).returncode


def toggle_fence_and_rebuild():
    # Match tests/run_tests.py behavior
    run([
        "sed", "-i", "s/#define FENCE.*/#define FENCE true/", "ha_lineairdb.cc",
    ], cwd=REPO_ROOT)
    # Rebuild only if Ninja files exist under build directory
    build_dir = os.path.join(REPO_ROOT, "build")
    build_ninja = os.path.join(build_dir, "build.ninja")
    if os.path.exists(build_ninja):
        run(["ninja", "-C", build_dir, "ha_lineairdb_storage_engine.so"])  # rebuild plugin only


# --- New: prepare datadir based on my.cnf ---

def parse_my_cnf(cnf_path: str) -> Tuple[str, str, str]:
    basedir = ""
    datadir = ""
    plugin_dir = ""
    with open(cnf_path, "r") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("["):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()
                if k == "basedir":
                    basedir = os.path.expanduser(v)
                elif k == "datadir":
                    datadir = os.path.expanduser(v)
                elif k == "plugin_dir":
                    plugin_dir = os.path.expanduser(v)
    # Resolve to absolute paths
    if basedir and not os.path.isabs(basedir):
        basedir = os.path.abspath(os.path.join(REPO_ROOT, basedir))
    if datadir and not os.path.isabs(datadir):
        # In my.cnf for this repo, datadir is usually relative to REPO_ROOT (e.g. ./build/data_workflows)
        # Avoid using basedir as prefix because it might duplicate paths (build/build/...)
        datadir = os.path.abspath(os.path.join(REPO_ROOT, datadir))
    if plugin_dir and not os.path.isabs(plugin_dir):
        # plugin_dir is often inside basedir, but checking REPO_ROOT first is safer for our layout
        plugin_dir = os.path.abspath(os.path.join(basedir or REPO_ROOT, plugin_dir))
    
    print(f"Parsed my.cnf: basedir={basedir}, datadir={datadir}, plugin_dir={plugin_dir}")
    return basedir, datadir, plugin_dir


def ensure_and_initialize_datadir():
    basedir, datadir, plugin_dir = parse_my_cnf(MY_CNF)
    if plugin_dir:
        os.makedirs(plugin_dir, exist_ok=True)

    # Skip initialize if it looks already initialized (presence of mysql system db dir)
    needs_init = True
    try:
        if datadir and os.path.isdir(os.path.join(datadir, "mysql")):
            needs_init = False
    except Exception:
        needs_init = True

    if needs_init:
        # If the directory exists but we need init, it might contain garbage or failed init files.
        # mysqld --initialize requires an empty directory.
        if datadir and os.path.exists(datadir):
            shutil.rmtree(datadir)
        if datadir:
            os.makedirs(datadir, exist_ok=True)

        run([
            MYSQLD,
            f"--defaults-file={MY_CNF}",
            "--initialize-insecure",
        ], env=MYSQLD_ENV)


# --- Existing start/stop and test runner ---

def start_mysqld():
    run([MYSQLD, f"--defaults-file={MY_CNF}", "--daemonize"], env=MYSQLD_ENV)
    time.sleep(2)


def stop_mysqld():
    subprocess.run(["pkill", "mysqld"])  # best-effort


def run_tests() -> int:
    exit_value = 0
    test_files = glob.glob(os.path.join(REPO_ROOT, "tests", "pytest", "*.py"))
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    for f in test_files:
        start_mysqld()
        code = subprocess.run([sys.executable, f, "--password", ""], env=env).returncode
        stop_mysqld()
        if code != 0:
            exit_value = code
    return exit_value


def main() -> int:
    toggle_fence_and_rebuild()
    ensure_and_initialize_datadir()
    return run_tests()


if __name__ == "__main__":
    sys.exit(main())
