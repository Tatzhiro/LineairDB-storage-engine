import os
import glob
import shutil
import subprocess
import sys
import time
import signal
import uuid
from typing import Tuple, Optional

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(REPO_ROOT, ".."))

MYSQLD = os.path.join(REPO_ROOT, "build", "bin", "mysqld")
MY_CNF_TEMPLATE = os.path.join(REPO_ROOT, "@github_workflows", "my.cnf.template")

# jemalloc path - only preload if it exists
JEMALLOC_PATH = "/lib/x86_64-linux-gnu/libjemalloc.so.2"


def get_mysqld_env() -> dict:
    """Create environment for mysqld with optional jemalloc preload."""
    env = os.environ.copy()
    if os.path.exists(JEMALLOC_PATH):
        env["LD_PRELOAD"] = JEMALLOC_PATH
        print(f"[INFO] LD_PRELOAD set to {JEMALLOC_PATH}")
    else:
        print(f"[WARN] jemalloc not found at {JEMALLOC_PATH}, skipping LD_PRELOAD")
    return env


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


def generate_my_cnf(test_id: str) -> Tuple[str, str, str]:
    """Generate my.cnf from template with absolute paths.
    
    Returns (my_cnf_path, datadir, socket_path)
    """
    basedir = os.path.join(REPO_ROOT, "build")
    datadir = os.path.join(REPO_ROOT, "build", f"data_ci_{test_id}")
    socket_path = f"/tmp/mysql-ci-{test_id}.sock"
    plugin_dir = os.path.join(basedir, "plugin_output_directory")
    log_error = os.path.join(REPO_ROOT, "build", f"mysqld-ci-{test_id}.err")
    
    my_cnf_path = os.path.join(REPO_ROOT, "build", f"my-ci-{test_id}.cnf")
    
    # Read template
    with open(MY_CNF_TEMPLATE, 'r') as f:
        template = f.read()
    
    # Substitute placeholders
    content = template.replace("__BASEDIR__", basedir)
    content = content.replace("__DATADIR__", datadir)
    content = content.replace("__SOCKET__", socket_path)
    content = content.replace("__PLUGIN_DIR__", plugin_dir)
    content = content.replace("__LOG_ERROR__", log_error)
    
    # Write generated config
    with open(my_cnf_path, 'w') as f:
        f.write(content)
    
    print(f"[INFO] Generated my.cnf at {my_cnf_path}")
    print(f"[INFO]   basedir: {basedir}")
    print(f"[INFO]   datadir: {datadir}")
    print(f"[INFO]   plugin_dir: {plugin_dir}")
    print(f"[INFO]   socket: {socket_path}")
    
    return my_cnf_path, datadir, socket_path


def verify_plugin_files():
    """Verify that required plugin files exist before starting tests."""
    plugin_dir = os.path.join(REPO_ROOT, "build", "plugin_output_directory")
    required_plugins = [
        "ha_lineairdb_storage_engine.so",
        "component_reference_cache.so",
    ]
    
    print("[INFO] Verifying plugin files...")
    for plugin in required_plugins:
        plugin_path = os.path.join(plugin_dir, plugin)
        if os.path.exists(plugin_path):
            print(f"[OK] {plugin} exists")
        else:
            print(f"[ERROR] {plugin} NOT FOUND at {plugin_path}")
            # List directory contents for debugging
            if os.path.exists(plugin_dir):
                print(f"[DEBUG] Contents of {plugin_dir}:")
                for f in os.listdir(plugin_dir)[:20]:
                    print(f"        - {f}")
            return False
    return True


def initialize_datadir(my_cnf: str, datadir: str, env: dict):
    """Initialize MySQL data directory if needed."""
    mysql_dir = os.path.join(datadir, "mysql")
    
    if os.path.isdir(mysql_dir):
        print(f"[INFO] Datadir already initialized: {datadir}")
        return
    
    # Clean up any partial initialization
    if os.path.exists(datadir):
        print(f"[INFO] Removing old datadir: {datadir}")
        shutil.rmtree(datadir)
    
    os.makedirs(datadir, exist_ok=True)
    
    print(f"[INFO] Initializing datadir: {datadir}")
    run([
        MYSQLD,
        f"--defaults-file={my_cnf}",
        "--initialize-insecure",
    ], env=env)


def wait_for_socket(socket_path: str, timeout: int = 30) -> bool:
    """Wait for MySQL socket to become available."""
    print(f"[INFO] Waiting for socket {socket_path}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        if os.path.exists(socket_path):
            print(f"[OK] Socket is ready after {time.time() - start_time:.1f}s")
            return True
        time.sleep(0.5)
    print(f"[ERROR] Socket not ready after {timeout}s")
    return False


def find_mysqld_pid(socket_path: str) -> Optional[int]:
    """Find the PID of mysqld using the given socket."""
    try:
        result = subprocess.run(
            ["lsof", "-t", socket_path],
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            # Return the first PID (mysqld process)
            return int(result.stdout.strip().split('\n')[0])
    except Exception as e:
        print(f"[WARN] Could not find mysqld PID: {e}")
    return None


def start_mysqld(my_cnf: str, socket_path: str, env: dict) -> bool:
    """Start mysqld and wait for it to be ready."""
    # Clean up stale socket if exists
    if os.path.exists(socket_path):
        os.remove(socket_path)
        print(f"[INFO] Removed stale socket: {socket_path}")
    
    print("[INFO] Starting mysqld...")
    run([MYSQLD, f"--defaults-file={my_cnf}", "--daemonize"], env=env)
    
    if not wait_for_socket(socket_path, timeout=60):
        print("[ERROR] mysqld failed to start")
        # Print error log for debugging
        log_pattern = my_cnf.replace(".cnf", ".err").replace("my-ci-", "mysqld-ci-")
        if os.path.exists(log_pattern):
            print(f"[DEBUG] Error log ({log_pattern}):")
            with open(log_pattern, 'r') as f:
                for line in f.readlines()[-50:]:
                    print(f"        {line.rstrip()}")
        return False
    
    return True


def stop_mysqld(socket_path: str, timeout: int = 30) -> bool:
    """Stop mysqld gracefully and wait for it to exit."""
    pid = find_mysqld_pid(socket_path)
    
    if pid is None:
        # Try pkill as fallback
        subprocess.run(["pkill", "-f", socket_path], check=False)
        time.sleep(2)
        return True
    
    print(f"[INFO] Stopping mysqld (PID: {pid})...")
    
    try:
        # Send SIGTERM for graceful shutdown
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        print("[INFO] mysqld already stopped")
        return True
    
    # Wait for process to exit
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            os.kill(pid, 0)  # Check if process exists
            time.sleep(0.5)
        except ProcessLookupError:
            print(f"[OK] mysqld stopped after {time.time() - start_time:.1f}s")
            return True
    
    # Force kill if still running
    print(f"[WARN] mysqld did not stop gracefully, sending SIGKILL")
    try:
        os.kill(pid, signal.SIGKILL)
        time.sleep(1)
    except ProcessLookupError:
        pass
    
    return True


def cleanup_test_environment(test_id: str, datadir: str, socket_path: str, my_cnf: str):
    """Clean up test environment after a test run."""
    # Remove socket file
    if os.path.exists(socket_path):
        try:
            os.remove(socket_path)
        except Exception:
            pass
    
    # Optionally remove datadir (uncomment if needed)
    # if os.path.exists(datadir):
    #     shutil.rmtree(datadir)
    
    # Keep my.cnf and logs for debugging


def run_single_test(test_file: str, test_id: str, env: dict) -> int:
    """Run a single test file with its own mysqld instance."""
    print(f"\n{'='*60}")
    print(f"[TEST] Running: {os.path.basename(test_file)}")
    print(f"{'='*60}")
    
    # Generate unique my.cnf for this test
    my_cnf, datadir, socket_path = generate_my_cnf(test_id)
    mysqld_env = get_mysqld_env()
    
    try:
        # Initialize datadir
        initialize_datadir(my_cnf, datadir, mysqld_env)
        
        # Start mysqld
        if not start_mysqld(my_cnf, socket_path, mysqld_env):
            return 1
        
        # Run the test
        # mysql.connector uses MYSQL_UNIX_PORT env var if available
        test_env = env.copy()
        test_env["MYSQL_UNIX_PORT"] = socket_path
        test_env.setdefault("PYTHONUNBUFFERED", "1")
        
        print(f"[INFO] Executing test: {test_file}")
        print(f"[INFO] MYSQL_UNIX_PORT={socket_path}")
        
        # Call test without --socket (not supported by all tests)
        code = subprocess.run(
            [sys.executable, test_file, "--password", ""],
            env=test_env
        ).returncode
        
        return code
        
    finally:
        # Always stop mysqld
        stop_mysqld(socket_path)
        cleanup_test_environment(test_id, datadir, socket_path, my_cnf)


def run_tests() -> int:
    """Run all test files."""
    exit_value = 0
    test_files = sorted(glob.glob(os.path.join(REPO_ROOT, "tests", "pytest", "*.py")))
    
    if not test_files:
        print("[WARN] No test files found!")
        return 0
    
    print(f"[INFO] Found {len(test_files)} test files")
    env = os.environ.copy()
    
    for i, test_file in enumerate(test_files):
        # Generate unique ID for each test
        test_id = f"{i}_{uuid.uuid4().hex[:8]}"
        
        code = run_single_test(test_file, test_id, env)
        
        if code != 0:
            print(f"[FAIL] Test failed with exit code: {code}")
            exit_value = code
        else:
            print(f"[PASS] Test completed successfully")
    
    return exit_value


def main() -> int:
    print(f"[INFO] Repository root: {REPO_ROOT}")
    print(f"[INFO] mysqld: {MYSQLD}")
    
    # Verify plugin files exist
    if not verify_plugin_files():
        print("[FATAL] Required plugin files are missing!")
        return 1
    
    toggle_fence_and_rebuild()
    return run_tests()


if __name__ == "__main__":
    sys.exit(main())
