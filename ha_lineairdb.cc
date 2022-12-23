/* Copyright (c) 2004, 2021, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_lineairdb.cc

  @brief
  The ha_lineairdb engine is a stubbed storage engine for lineairdb purposes
  only; it does nothing at this point. Its purpose is to provide a source code
  illustration of how to begin writing new storage engines; see also
  /storage/lineairdb/ha_lineairdb.h.

  @details
  ha_lineairdb will let you create/open/delete tables, but
  nothing further (for lineairdb, indexes are not supported nor can data
  be stored in the table). Use this lineairdb as a template for
  implementing the same functionality in your own storage engine. You
  can enable the lineairdb storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-lineairdb-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE \<table name\> (...) ENGINE=LINEAIRDB;

  The lineairdb storage engine is set up to use table locks. It
  implements an lineairdb "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  lineairdb handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_lineairdb.h before reading the rest
  of this file.

  @note
  When you create an LINEAIRDB table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an lineairdb select that would do a scan of an entire
  table:

  @code
  ha_lineairdb::store_lock
  ha_lineairdb::external_lock
  ha_lineairdb::info
  ha_lineairdb::rnd_init
  ha_lineairdb::extra
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::rnd_next
  ha_lineairdb::extra
  ha_lineairdb::external_lock
  ha_lineairdb::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the lineairdb storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_lineairdb::open() would also have been necessary. Calls to
  ha_lineairdb::extra() are hints as to what will be occurring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include "storage/lineairdb/ha_lineairdb.h"

#include <bitset>
#include <array>
#include <iostream>

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"

#define BLOB_MEMROOT_ALLOC_SIZE (8192)
#define BYTE_BIT_NUMBER (8)
#define FENCE true

LineairDB_share::LineairDB_share() {
  thr_lock_init(&lock);
  if (lineairdb_ == nullptr) {
    LineairDB::Config conf;
    conf.enable_checkpointing = false;
    conf.enable_recovery      = false;
    conf.max_thread           = 1;
    lineairdb_.reset(new LineairDB::Database(conf));
  }
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each lineairdb handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

LineairDB_share* ha_lineairdb::get_share() {
  LineairDB_share* tmp_share;

  DBUG_TRACE;

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<LineairDB_share*>(get_ha_share_ptr()))) {
    tmp_share = new LineairDB_share;
    if (!tmp_share) goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  return tmp_share;
}

LineairDB::Database* ha_lineairdb::get_db() {
  return get_share()->lineairdb_.get();
}

static PSI_memory_key csv_key_memory_blobroot;

ha_lineairdb::ha_lineairdb(handlerton* hton, TABLE_SHARE* table_arg)
    : handler(hton, table_arg),
      current_position_(0),
      blobroot(csv_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_lineairdb::open(const char*, int, uint, const dd::Table*) {
  DBUG_TRACE;
  if (!(share = get_share())) return 1;
  thr_lock_data_init(&share->lock, &lock, nullptr);

  return 0;
}

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_lineairdb::close(void) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  write_row() inserts a row.
  No extra() hint is given currently if a bulk load is happening.
  @param buf is a byte array of data.
*/
int ha_lineairdb::write_row(uchar* buf) {
  DBUG_TRACE;

  set_current_key();
  set_write_buffer(buf);

  auto& tx = get_db()->BeginTransaction();
  tx.Write(get_current_key(), reinterpret_cast<std::byte*>(write_buffer_.ptr()),
           write_buffer_.length());
  get_db()->EndTransaction(tx, [&](auto) {});
#if FENCE
  get_db()->Fence();
#endif

  return 0;
}

int ha_lineairdb::update_row(const uchar*, uchar* buf) {
  DBUG_TRACE;

  set_current_key();
  set_write_buffer(buf);

  auto& tx = get_db()->BeginTransaction();

  tx.Write(get_current_key(), reinterpret_cast<std::byte*>(write_buffer_.ptr()),
           write_buffer_.length());
  get_db()->EndTransaction(tx, [&](auto) {});
#if FENCE
  get_db()->Fence();
#endif

  return 0;
}

int ha_lineairdb::delete_row(const uchar*) {
  DBUG_TRACE;

  set_current_key();

  auto& tx = get_db()->BeginTransaction();
  tx.Write(get_current_key(), nullptr, 0);
  get_db()->EndTransaction(tx, [&](auto) {});

  return 0;
}

// MEMO: Return values of this function may be cached by MySQL internal
int ha_lineairdb::index_read_map(uchar* buf, const uchar* key, key_part_map,
                                 enum ha_rkey_function) {
  DBUG_TRACE;

  // NOTE:
  // Current implementation of lineairdb (049717)
  // supports only std::string for key of index.
  // Therefore, when MySQL requests to scan the storage
  // engine with unsupported key type (e.g., int),
  // we return HA_ERR_WRONG_COMMAND to indicate
  // "this key type is unsupported".
  const bool key_type_is_supported_by_lineairdb = true;

  if (!key_type_is_supported_by_lineairdb) return HA_ERR_WRONG_COMMAND;

  set_current_key(key);

  stats.records = 0;

  auto& tx         = get_db()->BeginTransaction();
  auto read_buffer = tx.Read(get_current_key());

  if (read_buffer.first == nullptr) {
    get_db()->EndTransaction(tx, [&](auto) {});
    return HA_ERR_END_OF_FILE;
  }
  if (set_fields_from_lineairdb(buf, read_buffer.first, read_buffer.second)) {
    tx.Abort();
    return HA_ERR_OUT_OF_MEM;
  }
  get_db()->EndTransaction(tx, [&](auto) {});
  return 0;
}

/**
  @brief
  Used to read forward through the index.
*/

int ha_lineairdb::index_next(uchar*) {
  DBUG_TRACE;
  return HA_ERR_END_OF_FILE;
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_lineairdb::index_prev(uchar*) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_lineairdb::index_first(uchar*) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_lineairdb::index_last(uchar*) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the lineairdb in the introduction at the top of this file to see
  when rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_lineairdb::rnd_init(bool) {
  DBUG_ENTER("ha_lineairdb::rnd_init");
  scanned_keys_.clear();
  current_position_ = 0;
  stats.records     = 0;

  auto& tx = get_db()->BeginTransaction();
  tx.Scan("", std::nullopt, [&](auto key, auto) {
    scanned_keys_.push_back(std::string(key));
    return false;
  });
  get_db()->EndTransaction(tx, [&](auto) {});
  DBUG_RETURN(0);
}

int ha_lineairdb::rnd_end() {
  DBUG_TRACE;
  blobroot.Clear();
  return 0;
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/

// assumption: takes 1 row
int ha_lineairdb::rnd_next(uchar* buf) {
  DBUG_ENTER("ha_lineairdb::rnd_next");
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  if (scanned_keys_.size() == 0) DBUG_RETURN(HA_ERR_END_OF_FILE);

read_from_lineairdb:
  if (current_position_ == scanned_keys_.size())
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  auto& key = scanned_keys_[current_position_];
  current_key_.set(key.c_str(), key.length(), current_key_.charset());

  auto& tx         = get_db()->BeginTransaction();
  auto read_buffer = tx.Read(key);

  if (read_buffer.first == nullptr) {
    get_db()->EndTransaction(tx, [&](auto) {});
    current_position_++;
    goto read_from_lineairdb;
  }
  if (set_fields_from_lineairdb(buf, read_buffer.first, read_buffer.second)) {
    tx.Abort();
    return HA_ERR_OUT_OF_MEM;
  }
  get_db()->EndTransaction(tx, [&](auto) {});
  current_position_++;
  DBUG_RETURN(0);
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_lineairdb::position(const uchar*) { DBUG_TRACE; }

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_lineairdb::rnd_pos(uchar*, uchar*) {
  int rc;
  DBUG_TRACE;
  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
*/
int ha_lineairdb::info(uint) {
  DBUG_TRACE;
  /* This is a lie, but you don't want the optimizer to see zero or 1 */
  if (stats.records < 2) stats.records = 2;
  return 0;
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_lineairdb::extra(enum ha_extra_function) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of an
  SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_query_block_query_expression::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_query_block_query_expression::exec() in sql_union.cc.
*/
int ha_lineairdb::delete_all_rows() {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_lineairdb::external_lock(THD*, int) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for lineairdb, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA** ha_lineairdb::store_lock(THD*, THR_LOCK_DATA** to,
                                         enum thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) lock.type = lock_type;
  *to++ = &lock;
  return to;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_lineairdb::delete_table(const char*, const dd::Table*) {
  DBUG_TRACE;
  /* This is not implemented but we want someone to be able that it works. */
  return 0;
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_lineairdb::rename_table(const char*, const char*, const dd::Table*,
                               dd::Table*) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_lineairdb::records_in_range(uint, key_range*, key_range*) {
  DBUG_TRACE;
  return 10;  // low number to force index usage
}

/**
  @brief
  create() is called to create a database. The variable name will have the
  name of the table.
  @see
  ha_create_table() in handle.cc
*/

int ha_lineairdb::create(const char*, TABLE*, HA_CREATE_INFO*, dd::Table*) {
  DBUG_TRACE;

  return 0;
}

/**
 * @brief This function only extracts the type of key for 
 *        tables that have single key
 * 
 * @return true Key type is int
 * @return false Key type is not int
 */
int ha_lineairdb::is_primary_key_type_int() {
  int bytes = 0;
  if (is_primary_key_exists()) {
    assert(table->s->keys == 1); 
    assert(max_supported_key_parts() ==
           1);  // now we assume that there is no composite index
    my_bitmap_map* org_bitmap = tmp_use_all_columns(table, table->read_set);
    for (Field** field = table->field; *field; field++) {
      auto* f = *field;
      if (f->m_indexed) {  // it is the key column
        ha_base_keytype key_type = f->key_type();
        switch (key_type) {
          case HA_KEYTYPE_SHORT_INT:
          case HA_KEYTYPE_USHORT_INT:
            bytes = sizeof(short);
            break;
          case HA_KEYTYPE_LONG_INT:
          case HA_KEYTYPE_ULONG_INT:
            bytes = sizeof(long);
            break;
          case HA_KEYTYPE_LONGLONG:
          case HA_KEYTYPE_ULONGLONG:
            bytes = sizeof(long long);
            break;
          case HA_KEYTYPE_INT24:
          case HA_KEYTYPE_UINT24:
            bytes = 3;
            break;
          case HA_KEYTYPE_INT8:
            bytes = sizeof(int8_t);
            break;
          default:
            bytes = 0;
            break;
        }
        break;
      }
    }
    tmp_restore_column_map(table->read_set, org_bitmap);
  }
  return bytes;
}

/**
 * @brief Set the current key of the row
 *  which is requested by the query.
 *  The key is formatted as the following:
 *    "table-[TABLENAME]-key-[KEY_STRING]"
 *
 * @param key
 *  By default, the part KEY_STRING is constructed from the fields of the
 * specified row, except for DELETE queries. For DELETE queries, pass the
 * key-object as `key` to extract KEY_STRING desired to delete.
 *
 * @WANTFIX:
 * The delimiter "-" here we use, is incomplete.
 * This char maybe exist in argument `table_name` and `key_name`.
 */
void ha_lineairdb::set_current_key(const uchar* key) {
  current_key_.length(0);
  {  // TABLE_NAME
    current_key_.append("table-");
    const auto& table_name = table->s->table_name;
    current_key_.append(table_name.str, table_name.length);
  }
  {
    // KEY_NAME
    current_key_.append("-key-");

    if (key != nullptr) {  // DELETE
      if (int int_bytes = is_primary_key_type_int()) {
        // for integer type keys
        int primary_key = 0;
        for (int i = 0; i < int_bytes; i++) {
          primary_key = primary_key | key[i] << sizeof(char) * i;
        }
        current_key_.append(std::to_string(primary_key));
      }
      else {
        auto pk_bytes = (key[1] << 8) | key[0];
        for (int i = 2; i < pk_bytes + 2; i++) { current_key_.append(key[i]); }
      }
    } else if (is_primary_key_exists()) {  // KEY EXISTS
      assert(max_supported_key_parts() ==
             1);  // now we assume that there is no composite index
      my_bitmap_map* org_bitmap = tmp_use_all_columns(table, table->read_set);
      for (Field** field = table->field; *field; field++) {
        auto* f = *field;
        if (f->m_indexed) {  // it is the key column

          String b;
          (*field)->val_str(&b, &b);
          current_key_.append(b);
          break;
        }
      }
      tmp_restore_column_map(table->read_set, org_bitmap);
    } else {  // THERE IS NO KEY COLUMN
      const auto cstr       = table->s->table_name;
      const auto table_name = std::string(cstr.str, cstr.length);
      auto inserted_count   = auto_generated_keys_[table_name]++;
      current_key_.append(std::to_string(inserted_count));
    }
  }
}

std::string ha_lineairdb::get_current_key() {
  return std::string(current_key_.ptr(), current_key_.length());
}

inline bool bit_is_up(uchar& flag, size_t idx){
  return (flag >> idx) & 1;
}

/**
 * @brief Format and set the requested row into `write_buffer_`.
 * A MySQL Row is serialized to comma-separated double-quoted values,
 * as the followings:
 * {"alice","bob","carol","3"}
 *   col1    col2  col3   col4
 */
void ha_lineairdb::set_write_buffer(uchar* buf) {
  write_buffer_.length(0);

  char attribute_buffer[1024];
  String attribute(attribute_buffer, sizeof(attribute_buffer), &my_charset_bin);

  translator.reset();

  my_bitmap_map* org_bitmap = tmp_use_all_columns(table, table->read_set);
  for (Field** field = table->field; *field; field++) {
    const char* p;
    const char* end;

    (*field)->val_str(&attribute, &attribute);
    p   = attribute.ptr();
    end = p + attribute.length();

    write_buffer_.append('"');
    for (; p < end; p++) write_buffer_.append(*p);
    write_buffer_.append('"');
    write_buffer_.append(',');
    if ((*field)->is_nullable()) translator.check_flag_length();
  }
  tmp_restore_column_map(table->read_set, org_bitmap);
  translator.save_null_flags(buf);
  write_buffer_.length(write_buffer_.length() - 1);
}

bool ha_lineairdb::is_primary_key_exists() { return (0 < table->s->keys); }

bool ha_lineairdb::store_blob_to_field(Field** field) {
  if ((*field)->is_flag_set(BLOB_FLAG)) {
    Field_blob* blob_field = down_cast<Field_blob*>(*field);
    size_t length          = blob_field->get_length();
    if (length > 0) {
      unsigned char* new_blob = new (&blobroot) unsigned char[length];
      if (new_blob == nullptr) return true;
      memcpy(new_blob, blob_field->get_blob_data(), length);
      blob_field->set_ptr(length, new_blob);
    }
  }
  return false;
}

void flush_null_flag_to_buf(uchar* buf, std::bitset<BYTE_BIT_NUMBER> &nullBit, 
                            size_t &nullable_field_index, size_t &buf_nullbyte_index) {
  uchar mask = nullBit.to_ulong();
  memcpy(&buf[buf_nullbyte_index], &mask, 1);
  buf_nullbyte_index++;
  nullable_field_index = 0;
  nullBit.set();
}

bool set_flag_for_nonnull_field(std::bitset<BYTE_BIT_NUMBER> &nullBit, 
                                const size_t &nullable_field_index,
                                String &write_buffer) {
  bool field_is_null = true;
  if (write_buffer.length() != 4 || strncmp(write_buffer.c_ptr(), "NULL", 4)) { 
    nullBit.flip(nullable_field_index); 
    field_is_null = false;
  }
  return field_is_null;
}

int ha_lineairdb::set_fields_from_lineairdb(uchar* buf,
                                            const std::byte* const read_buf,
                                            const size_t read_buf_size) {
  /* Avoid asserts in ::store() for columns that are not going to be updated
   */
  my_bitmap_map* org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  // Clear BLOB data from the previous row.
  blobroot.ClearForReuse();

  std::byte* p                  = (std::byte*)malloc(read_buf_size);
  const std::byte* const init_p = p;

  memcpy(p, read_buf, read_buf_size);
  std::byte* buf_end = p + read_buf_size;

  /**
   * extract values from LineairDB read_buf and 
   * store each column value to corresponding field
  */
  for (Field** field = table->field; *field; field++) {
    write_buffer_.length(0);
    for (; p < buf_end; p++) {
      uchar c              = *reinterpret_cast<uchar*>(p);
      bool is_end_of_field = p == buf_end - 1 ? true : false;
      switch (c) {
        case '\"':
          break;
        case ',':
          is_end_of_field = 1;
          break;
        default:
          write_buffer_.append(c);
          break;
      }
      if (is_end_of_field || p == buf_end) {
        (*field)->store(write_buffer_.ptr(), write_buffer_.length(),
                          write_buffer_.charset(), CHECK_FIELD_WARN);
        if (store_blob_to_field(field)) return HA_ERR_OUT_OF_MEM;
        p++;
        break;
      }
    }
  }

  write_buffer_.length(0);
  /**
   * for each 8 potentially null columns, buf holds 1 byte flag at the front
   * the number of null flag bytes in buf is shown in table->s->nullbytes
   * the flag is originally set to 0xff, or b11111111
   * if you want to make the first potentially null column to show a non-null
   * value, store 0xfe, or b11111110, in buf
  */
  translator.set_null_flags_in_buf(buf);

  free((void*)init_p);
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  return 0;
}
