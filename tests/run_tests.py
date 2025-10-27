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
  
  # テストファイルの決定：引数で指定された場合はそれを使用、なければ全てのテストを実行
  if args.tests:
    test_files = []
    for test in args.tests:
      # tests/pytest/ ディレクトリからの相対パスまたはファイル名を受け付ける
      if os.path.exists(test):
        test_files.append(test)
      elif os.path.exists(os.path.join("tests/pytest", test)):
        test_files.append(os.path.join("tests/pytest", test))
      elif os.path.exists(os.path.join("tests/pytest", f"{test}.py")):
        test_files.append(os.path.join("tests/pytest", f"{test}.py"))
      else:
        print(f"Warning: Test file not found: {test}")
  else:
    # 引数が指定されていない場合は全てのテストを実行
    test_files = glob.glob(os.path.join("tests/pytest", "*.py"))
  
  if not test_files:
    print("Error: No test files found")
    sys.exit(1)
    
  print(f"Running {len(test_files)} test(s):")
  for f in test_files:
    print(f"  - {f}")
  print()
  
  if is_ci: CI(test_files)
  else: run_tests(test_files)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Connect to MySQL')
  parser.add_argument('-c', '--ci', action='store_true',
                      help='run CI or default tests')
  parser.add_argument('tests', nargs='*',
                      help='specific test files to run (e.g., insert.py, select, tests/pytest/update.py). If not specified, all tests will be run.')
  args = parser.parse_args()
  main()