# How To Test

At first, you must build mysql-server in the `build` directory.

```bash
./build.sh
```

Then you have the following executables;

* build/bin/mysqld # server
* build/bin/mysql  # client
Before executing testing, you must launch with the initialization of mysql-server as the following:

```bash
# initialize
mkdir -p build/data
build/bin/mysqld --defaults-file=tests/my.cnf --initialize

# run mysql-server
build/bin/mysqld --defaults-file=tests/my.cnf
```

Next, see `build/my/error.log` and get the initial password of `root` user.
To testing, you should change the password to empty string:

```bash
mysqladmin -uroot -p'oldpassword' password ''
```

Then, install the generated plugin of this repository into mysql-server:

```mysql
mysql> install plugin lineairdb soname 'ha_lineairdb_storage_engine.so';
Query OK, 0 rows affected (0.80 sec)
```

At this time you can do testing.
To check all tests, execute the following command.
```
find tests/pytest/*.py | xargs -n 1 sh -c 'python3 $0'
```

For python tests, install mysql-connector-python. 
```
pip3 install -r tests/pytest/requirements.txt
```

