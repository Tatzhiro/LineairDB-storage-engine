import os
import glob

os.system("sed -i \"s/#define FENCE.*/#define FENCE true/\" ha_lineairdb.cc")
os.system("cd build; ninja")
py_files = glob.glob(os.path.join("tests/pytest", "*.py"))
for f in py_files:
  os.system("build/bin/mysqld --defaults-file=my.cnf --daemonize")
  os.system(f"python3 {f}")
  os.system("pkill mysqld")