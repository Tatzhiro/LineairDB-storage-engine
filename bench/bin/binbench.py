import os
import time
import subprocess
import sys
import argparse
import multiprocessing
from datetime import datetime
import re
from pathlib import Path
from plot import plot_benchmark_result

# --- Configuration & Paths ---
# Equivalent to base_path=$(pwd)/../..
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parents[1]
BENCHBASE_PATH = BASE_PATH / "third_party" / "benchbase"
BUILD_DIR = "build"
CNF_FILE = "my.cnf"

# Set environment variables
os.environ["LD_PRELOAD"] = "/lib/x86_64-linux-gnu/libjemalloc.so.2"


def main(benchmark, plot_name, engines, step):
    check_benchbase()
    
    now = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ex_file = BENCHBASE_PATH / "benchbase-mysql" / "benchbase.jar"
    
    # Define workload threads
    nproc = multiprocessing.cpu_count()
    num_threads = [1] + list(range(step, nproc + 1, step))

    restart_mysql()

    install_plugin()
    
    if benchmark == "ycsb":
        workloads = {
            "A": "50,0,0,50,0,0",
#            "B": "95,0,0,5,0,0",
#            "C": "100,0,0,0,0,0",
#            "E": "0,5,95,0,0,0",
#            "F": "50,0,0,0,0,50",
#            "write": "0,0,0,100,0,0"
        }
    elif benchmark == "tpcc":
        workloads = {
            "TPCC": "45,43,4,4,4"
        }

    for bm_type, weight in workloads.items():
        message(f"Starting Workload {bm_type}")
        
        # Update Benchbase XML config
        bench_config = BASE_PATH / f"bench/config/{benchmark}.xml"
        replace_in_file(bench_config, r"<weights>.*</weights>", f"<weights>{weight}</weights>")
        
        edit_fence(False)

        for engine in engines:
            set_storage_engine(engine)

            for threads in num_threads:
                message(f"Running Engine: {engine} | Threads: {threads}")
                
                # Set thread concurrency in my.cnf and benchbase config
                replace_in_file(BASE_PATH / CNF_FILE, r"innodb_thread_concurrency.*", f"innodb_thread_concurrency={threads}")
                replace_in_file(bench_config, r"<terminals>.*</terminals>", f"<terminals>{threads}</terminals>")

                restart_mysql()
                
                # Reset database
                with open(BASE_PATH / "bench/reset.sql", "r") as f:
                    subprocess.run([f"{BASE_PATH}/{BUILD_DIR}/bin/mysql", "-uroot"], stdin=f)

                # Execute BenchBase
                run_cmd(f"java -jar {ex_file} -b {benchmark} -c {BASE_PATH}/bench/config/{benchmark}.xml --create=true --load=true --execute=true", 
                        cwd=BENCHBASE_PATH)

                # Organize Results
                res_dir = BASE_PATH / "bench" / "results" / now / bm_type / engine / f"thread_{threads}"
                res_dir.mkdir(parents=True, exist_ok=True)
                
                # Move all CSVs from benchbase results folder to our timestamped folder
                for csv_file in (BENCHBASE_PATH / "results").glob("*.csv"):
                    csv_file.replace(res_dir / csv_file.name)

        # Plotting
        plot_dir = BASE_PATH / "bench" / "plots" / benchmark / bm_type
        plot_dir.mkdir(parents=True, exist_ok=True) 
        input_path = BASE_PATH / "bench" / "results" / now / bm_type
        
        plot_benchmark_result(engines, num_threads, input_path, plot_dir / f"{plot_name}_{bm_type}")

    message("Benchmarking Complete")


def message(text):
    print(f"\033[1m{text}\033[0m")


def run_cmd(cmd, cwd=None, shell=True, check=True, print_output=True):
    """Wrapper for subprocess to mimic 'set -x' and 'set -e'"""
    print(f"\033[90m+ {cmd}\033[0m") # Uncomment for 'set -x' style tracing
    try:
        res = subprocess.run(
            cmd, cwd=cwd, shell=shell, check=check,
            capture_output=True, text=True
        )
        if print_output and res.stdout:
            print(f"\033[90m{res.stdout}\033[0m")
        return res
    except subprocess.CalledProcessError as e:
        # This is where the magic happens:
        print("\033[91m[ERROR] Command failed!\033[0m")
        print(f"\033[91mSTDOUT:\033[0m\n{e.stdout}")
        print(f"\033[91mSTDERR:\033[0m\n{e.stderr}")
        # Re-raise if you want the script to stop, or return None to continue
        raise


def replace_in_file(file_path, search, replace):
    """Pythonic replacement for 'sed -i'"""
    content = file_path.read_text()
    new_content = subprocess.sub(search, replace, content) if hasattr(subprocess, 'sub') else content.replace(search, replace)
    # Using simple replace for fixed strings, or regex if needed:
    new_content = re.sub(search, replace, content)
    file_path.write_text(new_content)


def edit_fence(is_fencing: bool):
    is_fencing_str = "true" if is_fencing else "false"
    message(f"Editing Fence to {is_fencing_str} and recompiling...")
    
    replace_in_file(BASE_PATH / "ha_lineairdb.cc", r"#define FENCE.*", f"#define FENCE {is_fencing_str}")
    
    run_cmd(f"ninja lineairdb_storage_engine -j {multiprocessing.cpu_count()}", 
            cwd=BASE_PATH / BUILD_DIR)


def restart_mysql():
    message("Restarting MySQL...")
    # Find PID of running mysqld
    try:
        ps_output = run_cmd("ps -eo pid,args | grep '[m]ysqld' | grep -- '--defaults-file='").stdout
        pids = [line.strip().split()[0] for line in ps_output.splitlines()]
        
        for pid in pids:
            run_cmd(f"kill -9 {pid}")
            # Wait for process to actually die
            while True:
                try:
                    os.kill(int(pid), 0)
                    time.sleep(1)
                except OSError:
                    break
    except subprocess.CalledProcessError:
        pass # No mysqld running
    
    start_cmd = f"{BASE_PATH}/{BUILD_DIR}/bin/mysqld --defaults-file={BASE_PATH}/{CNF_FILE} --daemonize"
    subprocess.run(start_cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Wait for MySQL to be ready
    message("Waiting for MySQL to accept connections...")
    while True:
        try:
            run_cmd(f"{BASE_PATH}/{BUILD_DIR}/bin/mysqladmin -uroot ping")
            break
        except subprocess.CalledProcessError:
            time.sleep(1)
            
            
def install_plugin():
    check_engine = run_cmd(f"{BASE_PATH}/{BUILD_DIR}/bin/mysql -uroot -N -e 'SHOW ENGINES;'", print_output=False).stdout
    if "LINEAIRDB" not in check_engine:
        run_cmd(f"{BASE_PATH}/{BUILD_DIR}/bin/mysql -uroot -e \"INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';\"", print_output=False)
        
        
def set_storage_engine(engine):
    message(f"Setting storage engine to {engine}")
    # Update MySQL Config for storage engine
    if engine == "fence":
        edit_fence(True)
        replace_in_file(BASE_PATH / CNF_FILE, r"default_storage_engine.*", "default_storage_engine=lineairdb")
    else:
        replace_in_file(BASE_PATH / CNF_FILE, r"default_storage_engine.*", f"default_storage_engine={engine}")


def check_benchbase():
    """Verifies if BenchBase is compiled and ready for execution."""
    jar_path = BENCHBASE_PATH / "benchbase-mysql" / "benchbase.jar"
    
    if not jar_path.exists():
        message("ERROR: BenchBase is not installed or compiled.")
        print(f"Expected JAR at: {jar_path}")
        print("\nPlease run install_benchbase.sh in bench/bin directory:")
        
        # Exit the script because we cannot proceed without the JAR
        sys.exit(1)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmarking Script")
    parser.add_argument("benchmark", help="Benchmark type [ycsb|tpcc]")
    parser.add_argument("--plot_name", default="plot", help="Optional plot name")
    parser.add_argument("--engines", type=str, nargs='*',
                        help='storage engine to plot', default=["lineairdb","innodb"])
    parser.add_argument("--step", type=int, default=4,
                        help="step size of the number of threads")
    
    args = parser.parse_args()
    main(args.benchmark, args.plot_name, args.engines, args.step)
