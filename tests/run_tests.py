import os
import sys
import glob
import argparse

def CI(test_files):
  exit_value = 0
  for f in test_files:
    os.system("sudo systemctl restart mysql.service")
    if os.system(f"python3 {f} --password root"): exit_value = 1
  sys.exit(exit_value)

def run_tests(test_files):
  os.system("sed -i \"s/#define FENCE.*/#define FENCE true/\" ha_lineairdb.cc")
  os.system("cd build; ninja")
  for f in test_files:
    os.system("LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2 build/bin/mysqld --defaults-file=tests/my.cnf --daemonize")
    os.system(f"python3 {f}")
    os.system("pkill mysqld")

def main():
  # test
  is_ci = args.ci
  test_files = glob.glob(os.path.join("tests/pytest", "*.py"))
  if is_ci: CI(test_files)
  else: run_tests(test_files)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Connect to MySQL')
  parser.add_argument('-c', '--ci', action='store_true',
                      help='run CI or default tests')
  args = parser.parse_args()
  main()