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

#include "storage/lineairdb/ha_lineairdb.hh"

#include <algorithm>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>
#include <string_view>

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"
#include "storage/innobase/include/dict0mem.h"
#include "lineairdb_field_types.h"
#include "tpcc_stats.h"

#define BLOB_MEMROOT_ALLOC_SIZE (8192)
#define FENCE false

namespace
{
  constexpr unsigned char kKeyMarkerNotNull = 0x00;
  constexpr unsigned char kKeyMarkerNull = 0x01;

  constexpr unsigned char kKeyTypeInt = 0x10;
  constexpr unsigned char kKeyTypeString = 0x20;
  constexpr unsigned char kKeyTypeDatetime = 0x30;
  constexpr unsigned char kKeyTypeOther = 0xF0;
}

static std::shared_ptr<LineairDB::Database> get_or_allocate_database(
    LineairDB::Config conf);

void terminate_tx(LineairDBTransaction *&tx);
static int lineairdb_commit(handlerton *hton, THD *thd, bool shouldCommit);
static int lineairdb_abort(handlerton *hton, THD *thd, bool);

static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, nullptr,
                        nullptr, nullptr, nullptr);

static MYSQL_THDVAR_UINT(create_count_thdvar, 0, nullptr, nullptr, nullptr, 0,
                         0, 1000, 0);

// TPC-C mode variables (declared here for use in info() and records_in_range())
static bool srv_tpcc_mode = false;
static ulong srv_tpcc_warehouses = 1;

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_lineairdb_system_tables[] = {
    {(const char *)nullptr, (const char *)nullptr}};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @retval true   Given db.table_name is supported system table.
  @retval false  Given db.table_name is not a supported system table.
*/
static bool lineairdb_is_supported_system_table(
    const char *db, const char *table_name, bool is_sql_layer_system_table)
{
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab = ha_lineairdb_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db && strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

struct lineairdb_vars_t
{
  ulong var1;
  double var2;
  char var3[64];
  bool var4;
  bool var5;
  ulong var6;
};

static handler *lineairdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool partitioned, MEM_ROOT *mem_root);

handlerton *lineairdb_hton;

/* Interface to mysqld, to check system tables supported by SE */
static bool lineairdb_is_supported_system_table(const char *db,
                                                const char *table_name,
                                                bool is_sql_layer_system_table);

static handler *lineairdb_create_handler(handlerton *hton, TABLE_SHARE *table,
                                         bool, MEM_ROOT *mem_root)
{
  return new (mem_root) ha_lineairdb(hton, table);
}

static int lineairdb_init_func(void *p)
{
  DBUG_TRACE;

  lineairdb_hton = (handlerton *)p;
  lineairdb_hton->state = SHOW_OPTION_YES;
  lineairdb_hton->create = lineairdb_create_handler;
  lineairdb_hton->flags = HTON_CAN_RECREATE;
  lineairdb_hton->is_supported_system_table =
      lineairdb_is_supported_system_table;
  lineairdb_hton->db_type = DB_TYPE_UNKNOWN;
  lineairdb_hton->commit = lineairdb_commit;
  lineairdb_hton->rollback = lineairdb_abort;

  return 0;
}

static std::shared_ptr<LineairDB::Database> get_or_allocate_database(
    LineairDB::Config conf)
{
  static std::shared_ptr<LineairDB::Database> db;
  static std::once_flag flag;
  std::call_once(flag,
                 [&]()
                 { db = std::make_shared<LineairDB::Database>(conf); });
  return db;
}

LineairDB_share::LineairDB_share()
{
  thr_lock_init(&lock);
  next_hidden_pk.store(0);
  if (lineairdb_ == nullptr)
  {
    LineairDB::Config conf;
    conf.enable_checkpointing = false;
    conf.enable_recovery = false;
    conf.max_thread = 1;
    lineairdb_ = get_or_allocate_database(conf);
  }
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each lineairdb handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

LineairDB_share *ha_lineairdb::get_share()
{
  LineairDB_share *tmp_share;

  DBUG_TRACE;

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<LineairDB_share *>(get_ha_share_ptr())))
  {
    tmp_share = new LineairDB_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  return tmp_share;
}

LineairDB::Database *ha_lineairdb::get_db()
{
  return get_share()->lineairdb_.get();
}

static PSI_memory_key csv_key_memory_blobroot;

ha_lineairdb::ha_lineairdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      m_ds_mrr(this),
      current_position_(0),
      buffer_position_(0),
      last_batch_key_(),
      scan_exhausted_(false),
      blobroot(csv_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {}

void ha_lineairdb::set_key_and_key_part_info(const TABLE *const table)
{
  key_info = table->key_info;
  uint pk_index = table->s->primary_key;

  if (pk_index != MAX_KEY)
  {
    primary_key_type = static_cast<ha_base_keytype>(
        table->key_info[pk_index].key_part[0].type);

    key_part = table->key_info[pk_index].key_part;
    indexed_key_part = key_part[0];
    num_key_parts = table->key_info[pk_index].user_defined_key_parts;
  }
  else
  {
    primary_key_type = HA_KEYTYPE_END;
    key_part = nullptr;
    num_key_parts = 0;
  }
}

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

int ha_lineairdb::open(const char *table_name, int, uint, const dd::Table *)
{
  DBUG_TRACE;
  if (!(share = get_share()))
    return 1;
  thr_lock_data_init(&share->lock, &lock, nullptr);

  db_table_name = std::string(table_name);

  if ((num_keys = table->s->keys))
    set_key_and_key_part_info(table);

  if (table->s->primary_key != MAX_KEY)
  {
    uint pk_index = table->s->primary_key;
    ref_length = sizeof(uint16_t) + table->key_info[pk_index].key_length;
  }
  else
  {
    ref_length = sizeof(uint16_t) + serialize_hidden_primary_key(0).size();
  }

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

int ha_lineairdb::close(void)
{
  DBUG_TRACE;
  return 0;
}

int ha_lineairdb::change_active_index(uint keynr)
{
  DBUG_TRACE;
  active_index = keynr;

  if (table && table->s && keynr < table->s->keys)
  {
    current_index_name = std::string(table->key_info[keynr].name);
  }
  else
  {
    current_index_name.clear();
  }

  return 0;
}

int ha_lineairdb::index_init(uint idx, bool sorted [[maybe_unused]])
{
  DBUG_TRACE;
  current_position_in_index_ = 0;
  last_fetched_primary_key_.clear();
  prefix_cursor_.is_active = false;
  return change_active_index(idx);
}

int ha_lineairdb::index_end()
{
  DBUG_TRACE;
  active_index = MAX_KEY;
  prefix_cursor_.is_active = false;
  return 0;
}

int ha_lineairdb::index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag)
{
  DBUG_TRACE;
  return index_read_map(buf, key, HA_WHOLE_KEY, find_flag);
}

/**
  @brief
  write_row() inserts a row.
  No extra() hint is given currently if a bulk load is happening.
  @param buf is a byte array of data.
*/
int ha_lineairdb::write_row(uchar *buf)
{
  DBUG_TRACE;

  auto key = extract_key(buf);
  set_write_buffer(buf);

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  bool is_successful = tx->write(key, write_buffer_);
  if (!is_successful)
    return HA_ERR_LOCK_DEADLOCK;

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  for (uint i = 0; i < table->s->keys; i++)
  {
    auto key_info = table->key_info[i];
    if (i != table->s->primary_key)
    {
      // Use build_secondary_key_from_row to correctly read from buf instead of table->record[0]
      // This ensures thread-safety in multi-threaded environments
      std::string secondary_key = build_secondary_key_from_row(buf, key_info);

      bool is_successful = tx->write_secondary_index(key_info.name, secondary_key, key);
      if (!is_successful)
        return HA_ERR_LOCK_DEADLOCK;

      if (tx->is_aborted())
      {
        thd_mark_transaction_to_rollback(ha_thd(), 1);
        return HA_ERR_LOCK_DEADLOCK;
      }
    }
  }

  return 0;
}

int ha_lineairdb::update_row(const uchar *old_data, uchar *new_data)
{
  DBUG_TRACE;

  auto key = extract_key_from_mysql(old_data);

  if (key.empty())
  {
    key = last_fetched_primary_key_;
  }

  if (key.empty())
  {
    key = extract_primary_key_from_ref(ref);
  }

  last_fetched_primary_key_ = key;

  set_write_buffer(new_data);

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  bool is_successful = tx->write(key, write_buffer_);
  if (!is_successful)
    return HA_ERR_LOCK_DEADLOCK;

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  for (uint i = 0; i < table->s->keys; i++)
  {
    auto key_info = table->key_info[i];

    if (i == table->s->primary_key)
    {
      continue;
    }

    std::string old_secondary_key = build_secondary_key_from_row(old_data, key_info);
    std::string new_secondary_key = build_secondary_key_from_row(new_data, key_info);

    if (old_secondary_key == new_secondary_key)
    {
      continue;
    }

    tx->update_secondary_index(
        key_info.name,
        old_secondary_key,
        new_secondary_key,
        reinterpret_cast<const std::byte *>(key.data()),
        key.size());

    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }
  }

  return 0;
}

int ha_lineairdb::delete_row(const uchar *buf)
{
  DBUG_TRACE;

  auto key = extract_key_from_mysql(buf);

  if (key.empty())
  {
    key = last_fetched_primary_key_;
  }

  if (key.empty())
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  last_fetched_primary_key_ = key;

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  bool is_successful = tx->delete_value(key);
  if (!is_successful)
    return HA_ERR_LOCK_DEADLOCK;

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  for (uint i = 0; i < table->s->keys; i++)
  {
    auto key_info = table->key_info[i];
    if (i != table->s->primary_key)
    {
      // Use build_secondary_key_from_row to correctly read from buf instead of table->record[0]
      // This ensures thread-safety in multi-threaded environments
      std::string secondary_key = build_secondary_key_from_row(buf, key_info);

      bool is_successful = tx->delete_secondary_index(key_info.name, secondary_key, key);
      if (!is_successful)
        return HA_ERR_LOCK_DEADLOCK;

      if (tx->is_aborted())
      {
        thd_mark_transaction_to_rollback(ha_thd(), 1);
        return HA_ERR_LOCK_DEADLOCK;
      }
    }
  }
  return 0;
}

int ha_lineairdb::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                                 enum ha_rkey_function find_flag)
{
  DBUG_TRACE;

  stats.records = 0;
  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  secondary_index_results_.clear();
  current_position_in_index_ = 0;
  end_range_exclusive_key_.clear();
  prefix_cursor_.is_active = false;

  // Check if this is a prefix search (not all key parts are specified)
  KEY *key_info = &table->key_info[active_index];
  uint used_key_parts = count_used_key_parts(key_info, keypart_map);
  bool is_prefix_search = (used_key_parts < key_info->user_defined_key_parts);

  if (active_index == table->s->primary_key)
  {
    return index_read_primary_key(buf, key, keypart_map, find_flag,
                                  key_info, is_prefix_search, tx);
  }
  else
  {
    return index_read_secondary(buf, key, keypart_map, find_flag,
                                key_info, is_prefix_search, tx);
  }
}

/**
  @brief
  Used to read forward through the index.
*/

int ha_lineairdb::index_next(uchar *buf)
{
  DBUG_TRACE;
  if (secondary_index_results_.size() == 0)
  {
    return HA_ERR_END_OF_FILE;
  }

  if (current_position_in_index_ >= secondary_index_results_.size())
  {
    return HA_ERR_END_OF_FILE;
  }

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  std::string primary_key = secondary_index_results_[current_position_in_index_];
  auto result = tx->read(primary_key);
  if (set_fields_from_lineairdb(buf, result.first, result.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }
  current_position_in_index_++;
  last_fetched_primary_key_ = primary_key;
  return 0;
}

int ha_lineairdb::index_next_same(uchar *buf, const uchar *key, uint key_len)
{
  DBUG_TRACE;

  // Cursor-based prefix search handling
  if (prefix_cursor_.is_active)
  {
    if (prefix_cursor_.scan_exhausted)
    {
      return HA_ERR_END_OF_FILE;
    }

    auto tx = get_transaction(ha_thd());
    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    tx->choose_table(db_table_name);

    auto next_key = tx->fetch_next_key_with_prefix(
        prefix_cursor_.last_fetched_key, prefix_cursor_.prefix_end_key);

    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (!next_key.has_value())
    {
      prefix_cursor_.scan_exhausted = true;
      return HA_ERR_END_OF_FILE;
    }

    prefix_cursor_.last_fetched_key = next_key.value();

    auto result = tx->read(next_key.value());
    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }
    if (result.first == nullptr || result.second == 0)
    {
      return HA_ERR_KEY_NOT_FOUND;
    }

    if (set_fields_from_lineairdb(buf, result.first, result.second))
    {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }

    last_fetched_primary_key_ = next_key.value();
    return 0;
  }

  // Original secondary_index_results_ based handling
  if (secondary_index_results_.size() == 0)
  {
    return HA_ERR_END_OF_FILE;
  }

  if (current_position_in_index_ >= secondary_index_results_.size())
  {
    return HA_ERR_END_OF_FILE;
  }

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  std::string primary_key = secondary_index_results_[current_position_in_index_];
  auto result = tx->read(primary_key);
  if (set_fields_from_lineairdb(buf, result.first, result.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }
  current_position_in_index_++;
  last_fetched_primary_key_ = primary_key;
  return 0;
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_lineairdb::index_prev(uchar *)
{
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
int ha_lineairdb::index_first(uchar *buf)
{
  DBUG_TRACE;
  int error = index_read(buf, nullptr, 0, HA_READ_AFTER_KEY);

  /* MySQL does not seem to allow this to return HA_ERR_KEY_NOT_FOUND */

  if (error == HA_ERR_KEY_NOT_FOUND)
  {
    error = HA_ERR_END_OF_FILE;
  }

  return error;
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_lineairdb::index_last(uchar *)
{
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
int ha_lineairdb::rnd_init(bool)
{
  DBUG_ENTER("ha_lineairdb::rnd_init");
  scanned_keys_.clear();
  buffer_position_ = 0;
  last_batch_key_.clear();
  scan_exhausted_ = false;
  last_fetched_primary_key_.clear();
  current_position_ = 0;
  stats.records = 0;
  if (table->s->primary_key != MAX_KEY)
  {
    change_active_index(table->s->primary_key);
  }
  else
  {
    active_index = MAX_KEY;
  }

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
  }

  tx->choose_table(db_table_name);

  DBUG_RETURN(0);
}

int ha_lineairdb::rnd_end()
{
  DBUG_TRACE;
  scanned_keys_.clear();
  scanned_keys_.shrink_to_fit();
  buffer_position_ = 0;
  last_batch_key_.clear();
  scan_exhausted_ = false;
  blobroot.Clear();
  return 0;
}

bool ha_lineairdb::fetch_next_batch()
{
  DBUG_ENTER("ha_lineairdb::fetch_next_batch");

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted())
  {
    DBUG_RETURN(false);
  }

  scanned_keys_.clear();
  buffer_position_ = 0;
  scanned_keys_.reserve(SCAN_BATCH_SIZE);

  std::string begin = last_batch_key_;
  bool skip_first = !last_batch_key_.empty();

  tx->Scan(begin, std::nullopt,
           [&](std::string_view key, std::pair<const void *, const size_t> value)
           {
             if (skip_first)
             {
               if (key == begin)
               {
                 return false; // skip the last key of previous batch
               }
               skip_first = false;
             }

             // skip tombstone
             if (value.first == nullptr || value.second == 0)
             {
               return false;
             }

             scanned_keys_.emplace_back(key);
             if (scanned_keys_.size() >= SCAN_BATCH_SIZE)
             {
               return true; // stop scan
             }
             return false;
           });

  // Check if Scan was aborted due to conflict detection
  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    DBUG_RETURN(false);
  }

  if (scanned_keys_.empty())
  {
    DBUG_RETURN(false);
  }

  last_batch_key_ = scanned_keys_.back();
  DBUG_RETURN(true);
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
int ha_lineairdb::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_lineairdb::rnd_next");
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  if (buffer_position_ >= scanned_keys_.size())
  {
    if (scan_exhausted_)
    {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    if (!fetch_next_batch())
    {
      auto tx = get_transaction(ha_thd());
      if (tx->is_aborted())
      {
        DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
      }
      scan_exhausted_ = true;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }

  auto &key = scanned_keys_[buffer_position_];
  buffer_position_++;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  auto read_buffer = tx->read(key);
  int error = 0;
  if (read_buffer.first == nullptr)
  {
    error = HA_ERR_KEY_NOT_FOUND;
  }
  else
  {
    error = set_fields_from_lineairdb(buf, read_buffer.first, read_buffer.second);
    if (error == 0)
    {
      last_fetched_primary_key_ = key;
    }
  }
  current_position_++;
  DBUG_RETURN(error);
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
void ha_lineairdb::position(const uchar *)
{
  DBUG_TRACE;

  if (last_fetched_primary_key_.empty())
  {
    return;
  }

  store_primary_key_in_ref(last_fetched_primary_key_);
}

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
int ha_lineairdb::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_TRACE;

  std::string primary_key = extract_primary_key_from_ref(pos);

  if (primary_key.empty())
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  auto result = tx->read(primary_key);

  if (result.first == nullptr || result.second == 0)
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  if (set_fields_from_lineairdb(buf, result.first, result.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }

  last_fetched_primary_key_ = primary_key;

  return 0;
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
int ha_lineairdb::info(uint flag)
{
  DBUG_TRACE;

  // TPC-C mode: Use hardcoded statistics for optimizer
  if (srv_tpcc_mode && table != nullptr && table->s != nullptr)
  {
    const char *tbl_name = table->s->table_name.str;

    // HA_STATUS_VARIABLE: Set row count
    if (flag & HA_STATUS_VARIABLE)
    {
      uint64_t row_count = tpcc_stats::get_table_row_count(tbl_name, srv_tpcc_warehouses);
      if (row_count > 0)
      {
        stats.records = static_cast<ha_rows>(row_count);
      }
      else
      {
        // Unknown table, use default
        if (stats.records < 2)
          stats.records = 2;
      }
      /*        Along with records a few more variables you may wish to set are:
      　　　　　　　　　　　試しにrecordsに入れてみる
            records
            deleted
            data_file_length
            index_file_length
            delete_length
            check_time
          Take a look at the public variables in handler.h for more information. */

      // Estimate data file length
      stats.mean_rec_length = table->s->reclength > 0 ? table->s->reclength : 100;
      // 0にしてみる
      stats.data_file_length = stats.records * stats.mean_rec_length;
      // 0にしてみる
      stats.index_file_length = stats.data_file_length / 2;
    }

    // HA_STATUS_CONST: Set rec_per_key for each index
    if (flag & HA_STATUS_CONST)
    {
      set_tpcc_rec_per_key(tbl_name);
    }
  }
  else
  {
    // Default behavior for non-TPC-C mode
    if (stats.records < 2)
      stats.records = 2;
  }

  return 0;
}

/**
 * Set rec_per_key for TPC-C tables based on known data distribution
 * This helps the optimizer choose the correct index for TPC-C queries
 */
void ha_lineairdb::set_tpcc_rec_per_key(const char *table_name)
{
  if (table == nullptr || table->s == nullptr || table_name == nullptr)
    return;

  // Only process known TPC-C tables
  if (!tpcc_stats::is_tpcc_table(table_name))
    return;

  for (uint i = 0; i < table->s->keys; i++)
  {
    KEY *key = table->key_info + i;
    if (key == nullptr)
      continue;

    const char *key_name = key->name;
    uint key_parts = key->user_defined_key_parts;

    // Set rec_per_key based on table and index
    if (strcasecmp(table_name, "customer") == 0)
    {
      set_customer_rec_per_key(key, key_name, key_parts, i == table->s->primary_key);
    }
    else if (strcasecmp(table_name, "orders") == 0 ||
             strcasecmp(table_name, "oorder") == 0)
    {
      set_orders_rec_per_key(key, key_name, key_parts, i == table->s->primary_key);
    }
    else if (strcasecmp(table_name, "new_orders") == 0 ||
             strcasecmp(table_name, "new_order") == 0)
    {
      set_new_orders_rec_per_key(key, key_parts);
    }
    else if (strcasecmp(table_name, "stock") == 0)
    {
      set_stock_rec_per_key(key, key_parts);
    }
    else if (strcasecmp(table_name, "order_line") == 0)
    {
      set_order_line_rec_per_key(key, key_parts);
    }
    else
    {
      // Other tables: use generic heuristics
      set_generic_rec_per_key(key, key_parts, i == table->s->primary_key);
    }
  }
}

void ha_lineairdb::set_customer_rec_per_key(KEY *key, const char *key_name,
                                            uint key_parts, bool is_primary)
{
  // Check if this is the name index
  bool is_name_index = (key_name != nullptr &&
                        (strcasestr(key_name, "name") != nullptr ||
                         strcasestr(key_name, "idx_customer") != nullptr));

  if (is_primary || !is_name_index)
  {
    // PRIMARY KEY (c_w_id, c_d_id, c_id)
    ulong rpk[] = {30000, 3000, 1};
    for (uint j = 0; j < key_parts && j < 3; j++)
    {
      key->rec_per_key[j] = rpk[j];
      key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
    }
  }
  else
  {
    // idx_customer_name (c_w_id, c_d_id, c_last, c_first)
    // KEY POINT: c_last has much lower cardinality than PK!
    ulong rpk[] = {30000, 3000, 10, 1};
    for (uint j = 0; j < key_parts && j < 4; j++)
    {
      key->rec_per_key[j] = rpk[j];
      key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
    }
  }
}

void ha_lineairdb::set_orders_rec_per_key(KEY *key, const char *key_name,
                                          uint key_parts, bool is_primary)
{
  bool is_cid_index = (key_name != nullptr &&
                       (strcasestr(key_name, "c_id") != nullptr ||
                        strcasestr(key_name, "idx_orders") != nullptr));

  if (is_primary || !is_cid_index)
  {
    // PRIMARY KEY (o_w_id, o_d_id, o_id)
    ulong rpk[] = {30000, 3000, 1};
    for (uint j = 0; j < key_parts && j < 3; j++)
    {
      key->rec_per_key[j] = rpk[j];
      key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
    }
  }
  else
  {
    // Secondary index on customer ID
    ulong rpk[] = {30000, 3000, 10, 1};
    for (uint j = 0; j < key_parts && j < 4; j++)
    {
      key->rec_per_key[j] = rpk[j];
      key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
    }
  }
}

void ha_lineairdb::set_new_orders_rec_per_key(KEY *key, uint key_parts)
{
  // PRIMARY KEY (no_w_id, no_d_id, no_o_id)
  ulong rpk[] = {9000, 900, 1};
  for (uint j = 0; j < key_parts && j < 3; j++)
  {
    key->rec_per_key[j] = rpk[j];
    key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
  }
}

void ha_lineairdb::set_stock_rec_per_key(KEY *key, uint key_parts)
{
  // PRIMARY KEY (s_w_id, s_i_id)
  ulong rpk[] = {100000, 1};
  for (uint j = 0; j < key_parts && j < 2; j++)
  {
    key->rec_per_key[j] = rpk[j];
    key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
  }
}

void ha_lineairdb::set_order_line_rec_per_key(KEY *key, uint key_parts)
{
  // PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
  ulong rpk[] = {300000, 30000, 10, 1};
  for (uint j = 0; j < key_parts && j < 4; j++)
  {
    key->rec_per_key[j] = rpk[j];
    key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk[j]));
  }
}

void ha_lineairdb::set_generic_rec_per_key(KEY *key, uint key_parts, bool is_primary)
{
  // Generic heuristic for unknown indexes
  for (uint j = 0; j < key_parts; j++)
  {
    ulong rpk;
    if (is_primary && j == key_parts - 1)
    {
      rpk = 1; // Last part of primary key is unique
    }
    else
    {
      // Decrease by factor of 10 for each key part
      rpk = static_cast<ulong>(std::max(static_cast<ha_rows>(1),
                                        stats.records / ((j + 1) * 10)));
    }
    key->rec_per_key[j] = rpk;
    key->set_records_per_key(j, static_cast<rec_per_key_t>(rpk));
  }
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_lineairdb::extra(enum ha_extra_function)
{
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
int ha_lineairdb::delete_all_rows()
{
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
int ha_lineairdb::external_lock(THD *thd, int lock_type)
{
  DBUG_TRACE;

  // get_transaction() will automatically start the transaction if needed
  LineairDBTransaction *&tx = get_transaction(thd);

  const bool tx_is_ready_to_commit = lock_type == F_UNLCK;
  if (tx_is_ready_to_commit)
  {
    // tx may be nullptr for DDL operations like CREATE INDEX
    if (tx != nullptr && tx->is_a_single_statement())
    {
      lineairdb_commit(lineairdb_hton, thd, true);
    }
    return 0;
  }

  // Note: Transaction is already started in get_transaction()
  // This is intentional to handle cases where MySQL optimizer
  // calls index_read_map() before external_lock() (e.g., semi-join optimization)

  return 0;
}

int ha_lineairdb::start_stmt(THD *thd, thr_lock_type lock_type)
{
  assert(lock_type > 0);
  return external_lock(thd, lock_type);
}

/**
 * @brief Gets transaction from MySQL allocated memory
 *
 * This function follows the InnoDB pattern of "lazy transaction start".
 * The transaction is automatically started when first accessed, rather than
 * relying solely on external_lock() to start it.
 *
 * This is necessary because MySQL's query optimizer may call handler methods
 * (like index_read_map) before external_lock() in certain scenarios:
 * - Semi-join optimization
 * - Subquery materialization
 * - Complex JOIN operations
 *
 * Without this lazy start, accessing a transaction before external_lock()
 * would result in a nullptr dereference or assertion failure.
 */
LineairDBTransaction *&ha_lineairdb::get_transaction(THD *thd)
{
  LineairDBTransaction *&tx = *reinterpret_cast<LineairDBTransaction **>(
      thd_ha_data(thd, lineairdb_hton));
  if (tx == nullptr)
  {
    tx = new LineairDBTransaction(thd, get_db(), lineairdb_hton, FENCE);
  }
  if (tx->is_not_started())
  {
    tx->begin_transaction();
  }
  return tx;
}

/**
 * implementation of commit for lineairdb_hton
 */
static int lineairdb_commit(handlerton *hton, THD *thd, bool shouldTerminate)
{
  if (shouldTerminate == false)
    return 0;
  LineairDBTransaction *&tx =
      *reinterpret_cast<LineairDBTransaction **>(thd_ha_data(thd, hton));

  assert(tx != nullptr);

  terminate_tx(tx);
  return 0;
}

/**
 * implementation of rollback for lineairdb_hton
 */
static int lineairdb_abort(handlerton *hton, THD *thd, bool)
{
  LineairDBTransaction *&tx =
      *reinterpret_cast<LineairDBTransaction **>(thd_ha_data(thd, hton));

  assert(tx != nullptr);

  tx->set_status_to_abort();
  terminate_tx(tx);
  return 0;
}

void terminate_tx(LineairDBTransaction *&tx)
{
  tx->end_transaction();
  tx = nullptr;
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
THR_LOCK_DATA **ha_lineairdb::store_lock(THD *thd, THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    if (lock_type == TL_WRITE && !thd->in_lock_tables)
    {
      lock_type = TL_WRITE_ALLOW_WRITE;
    }
    lock.type = lock_type;
  }
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
int ha_lineairdb::delete_table(const char *, const dd::Table *)
{
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
int ha_lineairdb::rename_table(const char *, const char *, const dd::Table *,
                               dd::Table *)
{
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
ha_rows ha_lineairdb::records_in_range(uint inx, key_range *min_key,
                                       key_range *max_key)
{
  DBUG_TRACE;

  // TPC-C mode: Return optimized estimates for TPC-C tables
  if (srv_tpcc_mode && table != nullptr && table->s != nullptr)
  {
    const char *tbl_name = table->s->table_name.str;

    // Only handle known TPC-C tables
    if (tpcc_stats::is_tpcc_table(tbl_name))
    {
      KEY *key = table->key_info + inx;
      const char *key_name = key ? key->name : nullptr;
      bool is_primary = (inx == table->s->primary_key);

      // Calculate how many key parts are used in the range
      uint key_parts_used = 0;
      if (min_key != nullptr && key != nullptr)
      {
        key_parts_used = calculate_key_parts_from_length(key, min_key->length);
      }

      // Return TPC-C specific estimates
      return estimate_tpcc_records_in_range(tbl_name, key_name,
                                            key_parts_used, is_primary);
    }
  }

  // Default behavior for non-TPC-C mode
  return 10;
}

/**
 * Calculate how many key parts are covered by the given key length
 * This is an approximation based on key part sizes
 */
uint ha_lineairdb::calculate_key_parts_from_length(KEY *key, uint key_length)
{
  if (key == nullptr || key_length == 0)
    return 0;

  uint parts = 0;
  uint accumulated_length = 0;

  for (uint i = 0; i < key->user_defined_key_parts; i++)
  {
    KEY_PART_INFO *part = &key->key_part[i];

    // Add length for this key part (including null byte if nullable)
    uint part_length = part->store_length;
    accumulated_length += part_length;

    if (accumulated_length <= key_length)
    {
      parts++;
    }
    else
    {
      break;
    }
  }

  return parts;
}

/**
 * Estimate records in range for TPC-C tables
 * Returns appropriate estimates to favor secondary indexes when appropriate
 */
ha_rows ha_lineairdb::estimate_tpcc_records_in_range(const char *table_name,
                                                     const char *index_name,
                                                     uint key_parts_used,
                                                     bool is_primary)
{
  if (table_name == nullptr)
    return 10;

  // Customer table - most important for TPC-C optimization
  if (strcasecmp(table_name, "customer") == 0)
  {
    return tpcc_stats::estimate_customer_records_in_range(index_name, key_parts_used);
  }

  // Orders table
  if (strcasecmp(table_name, "orders") == 0 ||
      strcasecmp(table_name, "oorder") == 0)
  {
    return tpcc_stats::estimate_orders_records_in_range(index_name, key_parts_used);
  }

  // New orders table
  if (strcasecmp(table_name, "new_orders") == 0 ||
      strcasecmp(table_name, "new_order") == 0)
  {
    return tpcc_stats::estimate_new_orders_records_in_range(key_parts_used);
  }

  // Stock table
  if (strcasecmp(table_name, "stock") == 0)
  {
    return tpcc_stats::estimate_stock_records_in_range(key_parts_used);
  }

  // Order line table
  if (strcasecmp(table_name, "order_line") == 0)
  {
    return tpcc_stats::estimate_order_line_records_in_range(key_parts_used);
  }

  // Other tables: use default
  return 10;
}

/**
  @brief
  create() is called to create a database. The variable name will have the
  name of the table.
  @see
  ha_create_table() in handle.cc
*/

int ha_lineairdb::create(const char *table_name, TABLE *table, HA_CREATE_INFO *,
                         dd::Table *)
{
  DBUG_TRACE;
  db_table_name = std::string(table_name);
  auto current_db = get_db();
  if (!current_db->CreateTable(db_table_name))
  {
    return HA_ERR_TABLE_EXIST;
  }
  // define interface for create secondary index
  for (uint i = 0; i < table->s->keys; i++)
  {
    auto key_info = table->key_info[i];
    uint index_type = (key_info.flags & HA_NOSAME) ? DICT_UNIQUE : 0;
    if (i != table->s->primary_key)
    {
      // Now we don't assume composite index
      // TODO: need to convert mysql type to lineairdb type
      bool is_successful = current_db->CreateSecondaryIndex(db_table_name,
                                                            std::string(key_info.name),
                                                            index_type);
      if (!is_successful)
      {
        return HA_ERR_TABLE_EXIST;
      }
    }
  }
  return 0;
}

/**
  Check if inplace alter is supported for the given operation.
  Currently supports ADD_INDEX and ADD_UNIQUE_INDEX.
*/
enum_alter_inplace_result ha_lineairdb::check_if_supported_inplace_alter(
    TABLE *altered_table [[maybe_unused]],
    Alter_inplace_info *ha_alter_info)
{
  DBUG_TRACE;

  // Support ADD_INDEX and ADD_UNIQUE_INDEX operations
  Alter_inplace_info::HA_ALTER_FLAGS dominated_flags =
      Alter_inplace_info::ADD_INDEX |
      Alter_inplace_info::ADD_UNIQUE_INDEX;

  if (ha_alter_info->handler_flags & ~dominated_flags)
  {
    // Unsupported operation requested
    return HA_ALTER_INPLACE_NOT_SUPPORTED;
  }

  return HA_ALTER_INPLACE_EXCLUSIVE_LOCK;
}

bool ha_lineairdb::inplace_alter_table(
    TABLE *altered_table [[maybe_unused]],
    Alter_inplace_info *ha_alter_info,
    const dd::Table *old_table_def [[maybe_unused]],
    dd::Table *new_table_def [[maybe_unused]])
{
  DBUG_TRACE;

  auto current_db = get_db();

  for (uint i = 0; i < ha_alter_info->index_add_count; i++)
  {
    uint key_idx = ha_alter_info->index_add_buffer[i];
    KEY *key_info = &ha_alter_info->key_info_buffer[key_idx];

    uint index_type = (key_info->flags & HA_NOSAME) ? DICT_UNIQUE : 0;

    bool is_successful = current_db->CreateSecondaryIndex(
        db_table_name,
        std::string(key_info->name),
        index_type);

    if (!is_successful)
    {
      my_error(ER_DUP_KEYNAME, MYF(0), key_info->name);
      return true;
    }
  }

  return false;
}

ha_rows ha_lineairdb::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                                  void *seq_init_param, uint n_ranges,
                                                  uint *bufsz, uint *flags, bool *force_default_mrr,
                                                  Cost_estimate *cost)
{
  /* See comments in ha_myisam::multi_range_read_info_const */
  m_ds_mrr.init(table);

  return (m_ds_mrr.dsmrr_info_const(keyno, seq, seq_init_param, n_ranges, bufsz,
                                    flags, cost));
}

int ha_lineairdb::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                                        uint n_ranges, uint mode,
                                        HANDLER_BUFFER *buf)
{
  m_ds_mrr.init(table);
  return m_ds_mrr.dsmrr_init(seq, seq_init_param, n_ranges, mode, buf);
}

int ha_lineairdb::multi_range_read_next(char **range_info)
{
  return (m_ds_mrr.dsmrr_next(range_info));
}

int ha_lineairdb::read_range_first(const key_range *start_key, const key_range *end_key,
                                   bool eq_range_arg, bool sorted)
{
  return handler::read_range_first(start_key, end_key, eq_range_arg, sorted);
}

unsigned char ha_lineairdb::key_part_type_tag(LineairDBFieldType type)
{
  switch (type)
  {
  case LineairDBFieldType::LINEAIRDB_INT:
    return kKeyTypeInt;
  case LineairDBFieldType::LINEAIRDB_STRING:
    return kKeyTypeString;
  case LineairDBFieldType::LINEAIRDB_DATETIME:
    return kKeyTypeDatetime;
  case LineairDBFieldType::LINEAIRDB_OTHER:
  default:
    return kKeyTypeOther;
  }
}

void ha_lineairdb::append_key_part_encoding(std::string &out, bool is_null,
                                            LineairDBFieldType type,
                                            const std::string &payload)
{
  constexpr size_t kLengthFieldSize = 2;
  const size_t max_payload_length = std::numeric_limits<uint16_t>::max();
  size_t copy_length = std::min(payload.size(), max_payload_length);

  if (payload.size() > max_payload_length)
  {
    std::cerr << "[LineairDB][encode_key_part] payload truncated: length="
              << payload.size() << std::endl;
  }

  // Reserve for worst case (STRING type with terminator):
  // null_marker(1) + type_tag(1) + payload(copy_length) + terminator(1) + length(2) = 5 + copy_length
  // For other types: null_marker(1) + type_tag(1) + length(2) + payload(copy_length) = 4 + copy_length
  out.reserve(out.size() + 5 + copy_length);
  out.push_back(static_cast<char>(is_null ? kKeyMarkerNull : kKeyMarkerNotNull));
  out.push_back(static_cast<char>(key_part_type_tag(type)));

  // For STRING type, place payload BEFORE length to preserve lexicographic order.
  // For other types (INT, DATETIME), they are fixed-length so order doesn't matter.
  // Format for STRING: [null_marker][type_tag][payload][0x00][length_high][length_low]
  // Format for others: [null_marker][type_tag][length_high][length_low][payload]
  if (type == LineairDBFieldType::LINEAIRDB_STRING)
  {
    // STRING: payload first, then terminator (0x00), then length
    if (copy_length > 0)
    {
      out.append(payload.data(), copy_length);
    }
    out.push_back('\0'); // terminator to ensure shorter strings sort before longer ones with same prefix
    uint16_t length_field = static_cast<uint16_t>(copy_length);
    out.push_back(static_cast<char>((length_field >> 8) & 0xFF));
    out.push_back(static_cast<char>(length_field & 0xFF));
  }
  else
  {
    // INT, DATETIME, OTHER: length first, then payload (fixed-length types)
    uint16_t length_field = static_cast<uint16_t>(copy_length);
    out.push_back(static_cast<char>((length_field >> 8) & 0xFF));
    out.push_back(static_cast<char>(length_field & 0xFF));

    if (copy_length > 0)
    {
      out.append(payload.data(), copy_length);
    }
  }
}

std::string ha_lineairdb::build_prefix_range_end(const std::string &prefix)
{
  std::string end = prefix;
  end.push_back(static_cast<char>(0xFF));
  return end;
}

/**
 * @brief Count the number of key parts used in a key_part_map
 *
 * @param key_info KEY structure containing key part information
 * @param keypart_map Bitmap indicating which key parts are used
 * @return Number of consecutive key parts used (from the beginning)
 */
uint ha_lineairdb::count_used_key_parts(const KEY *key_info, key_part_map keypart_map)
{
  uint count = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++)
  {
    if ((keypart_map >> i) & 1)
      count++;
    else
      break;
  }
  return count;
}

/**
 * @brief Fetch and set the current result from secondary_index_results_
 *
 * This helper function reads the primary key at current_position_in_index_,
 * fetches the data from LineairDB, and sets the fields in the buffer.
 *
 * @param buf Buffer to store the result
 * @param tx Transaction object
 * @return 0 on success, error code on failure
 */
int ha_lineairdb::fetch_and_set_current_result(uchar *buf, LineairDBTransaction *tx)
{
  if (secondary_index_results_.empty())
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  std::string primary_key = secondary_index_results_[current_position_in_index_];

  tx->choose_table(db_table_name);

  auto result = tx->read(primary_key);

  if (result.first == nullptr || result.second == 0)
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  if (set_fields_from_lineairdb(buf, result.first, result.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }

  current_position_in_index_++;
  last_fetched_primary_key_ = primary_key;
  return 0;
}

/**
 * @brief Handle PRIMARY KEY index read operations
 *
 * This function handles all PRIMARY KEY search operations including:
 * - Full scan (key == nullptr)
 * - Exact match search
 * - Prefix/range search with various find_flag values
 *
 * @param buf Buffer to store the result
 * @param key Search key (nullptr for full scan)
 * @param keypart_map Bitmap indicating which key parts are used
 * @param find_flag Search mode (HA_READ_KEY_EXACT, HA_READ_AFTER_KEY, etc.)
 * @param key_info KEY structure for the active index
 * @param is_prefix_search True if not all key parts are specified
 * @param tx Transaction object
 * @return 0 on success, error code on failure
 */
int ha_lineairdb::index_read_primary_key(uchar *buf, const uchar *key, key_part_map keypart_map,
                                         enum ha_rkey_function find_flag, KEY *key_info,
                                         bool is_prefix_search, LineairDBTransaction *tx)
{
  // Full scan: key == nullptr
  if (key == nullptr)
  {
    std::string serialized_start_key = "";
    std::string serialized_end_key;

    if (end_range != nullptr)
    {
      serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

      // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
      if (end_range->flag == HA_READ_BEFORE_KEY)
      {
        end_range_exclusive_key_ = serialized_end_key;
      }
    }
    else
    {
      serialized_end_key = std::string(8, '\xFF');
    }

    secondary_index_results_ = tx->get_matching_keys_in_range(
        serialized_start_key, serialized_end_key, end_range_exclusive_key_);

    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (secondary_index_results_.empty())
    {
      return HA_ERR_END_OF_FILE;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  auto serialized_key = convert_key_to_ldbformat(key, keypart_map);

  // Exact match search
  if (end_range == nullptr && !is_prefix_search && find_flag == HA_READ_KEY_EXACT)
  {
    auto result = tx->read(serialized_key);

    if (result.first == nullptr || result.second == 0)
    {
      return HA_ERR_KEY_NOT_FOUND;
    }

    if (set_fields_from_lineairdb(buf, result.first, result.second))
    {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }

    secondary_index_results_.push_back(serialized_key);
    current_position_in_index_ = 1;
    last_fetched_primary_key_ = serialized_key;

    return 0;
  }

  // Cursor-based prefix search (for LIMIT optimization)
  // This handles: end_range == nullptr && is_prefix_search && find_flag == HA_READ_KEY_EXACT
  if (end_range == nullptr && is_prefix_search && find_flag == HA_READ_KEY_EXACT)
  {
    // Initialize cursor state
    prefix_cursor_.is_active = true;
    prefix_cursor_.prefix_key = serialized_key;
    prefix_cursor_.prefix_end_key = build_prefix_range_end(serialized_key);
    prefix_cursor_.scan_exhausted = false;

    // Fetch the first matching key
    auto first_key = tx->fetch_first_key_with_prefix(
        prefix_cursor_.prefix_key, prefix_cursor_.prefix_end_key);

    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (!first_key.has_value())
    {
      prefix_cursor_.is_active = false;
      return HA_ERR_KEY_NOT_FOUND;
    }

    prefix_cursor_.last_fetched_key = first_key.value();

    // Read the row data
    auto result = tx->read(first_key.value());
    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }
    if (result.first == nullptr || result.second == 0)
    {
      prefix_cursor_.is_active = false;
      return HA_ERR_KEY_NOT_FOUND;
    }

    if (set_fields_from_lineairdb(buf, result.first, result.second))
    {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }

    last_fetched_primary_key_ = first_key.value();
    return 0;
  }

  // PRIMARY KEY prefix/range search
  std::string serialized_end_key;
  std::string effective_start_key = serialized_key;

  if (find_flag == HA_READ_AFTER_KEY)
  {
    // Exclude start key by appending a byte to search after it
    effective_start_key.push_back('\x00');
    if (end_range != nullptr)
    {
      serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

      // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
      if (end_range->flag == HA_READ_BEFORE_KEY)
      {
        // Exclusive end: do not extend prefix - scan ends before this key
        end_range_exclusive_key_ = serialized_end_key;
      }
      else
      {
        // Inclusive end: extend prefix to include all keys with this prefix
        uint end_used_key_parts = count_used_key_parts(key_info, end_range->keypart_map);
        if (end_used_key_parts < key_info->user_defined_key_parts)
        {
          serialized_end_key = build_prefix_range_end(serialized_end_key);
        }
      }
    }
    else
    {
      serialized_end_key = std::string(effective_start_key.size() + 1, '\xFF');
    }
  }
  else if (find_flag == HA_READ_KEY_OR_NEXT)
  {
    if (end_range != nullptr)
    {
      serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

      // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
      if (end_range->flag == HA_READ_BEFORE_KEY)
      {
        // Exclusive end: do not extend prefix - scan ends before this key
        end_range_exclusive_key_ = serialized_end_key;
      }
      else
      {
        // Inclusive end: extend prefix to include all keys with this prefix
        uint end_used_key_parts = count_used_key_parts(key_info, end_range->keypart_map);
        if (end_used_key_parts < key_info->user_defined_key_parts)
        {
          serialized_end_key = build_prefix_range_end(serialized_end_key);
        }
      }
    }
    else
    {
      serialized_end_key = std::string(serialized_key.size() + 1, '\xFF');
    }
  }
  else if (end_range != nullptr)
  {
    serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

    // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
    if (end_range->flag == HA_READ_BEFORE_KEY)
    {
      // Exclusive end: do not extend prefix - scan ends before this key
      end_range_exclusive_key_ = serialized_end_key;
    }
    else
    {
      // Inclusive end: extend prefix to include all keys with this prefix
      uint end_used_key_parts = count_used_key_parts(key_info, end_range->keypart_map);

      // Extend if either: start key is prefix (and same as end), or end key itself is prefix
      if ((is_prefix_search && serialized_end_key == serialized_key) ||
          end_used_key_parts < key_info->user_defined_key_parts)
      {
        serialized_end_key = build_prefix_range_end(serialized_end_key);
      }
    }
  }
  else
  {
    serialized_end_key = build_prefix_range_end(serialized_key);
  }

  // Only extend if not exclusive end - exclusive end should use original key as boundary
  if (serialized_end_key.size() < effective_start_key.size() && end_range_exclusive_key_.empty())
  {
    serialized_end_key = build_prefix_range_end(serialized_end_key);
  }

  secondary_index_results_ = tx->get_matching_keys_in_range(
      effective_start_key, serialized_end_key, end_range_exclusive_key_);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty())
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief Handle SECONDARY INDEX read operations
 *
 * This function handles all SECONDARY INDEX search operations including:
 * - Full scan (key == nullptr)
 * - Exact match search
 * - Prefix/range search with various find_flag values
 *
 * Note: Unlike PRIMARY KEY, SECONDARY INDEX does not perform prefix extension
 * checks on end_range.
 *
 * @param buf Buffer to store the result
 * @param key Search key (nullptr for full scan)
 * @param keypart_map Bitmap indicating which key parts are used
 * @param find_flag Search mode (HA_READ_KEY_EXACT, HA_READ_AFTER_KEY, etc.)
 * @param key_info KEY structure for the active index
 * @param is_prefix_search True if not all key parts are specified
 * @param tx Transaction object
 * @return 0 on success, error code on failure
 */
int ha_lineairdb::index_read_secondary(uchar *buf, const uchar *key, key_part_map keypart_map,
                                       enum ha_rkey_function find_flag, KEY *key_info [[maybe_unused]],
                                       bool is_prefix_search, LineairDBTransaction *tx)
{
  // Full scan: key == nullptr
  if (key == nullptr)
  {
    std::string serialized_start_key = "";
    std::string serialized_end_key;

    if (end_range != nullptr)
    {
      serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

      // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
      if (end_range->flag == HA_READ_BEFORE_KEY)
      {
        end_range_exclusive_key_ = serialized_end_key;
      }
    }
    else
    {
      serialized_end_key = std::string(8, '\xFF');
    }

    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, serialized_start_key, serialized_end_key, end_range_exclusive_key_);

    if (tx->is_aborted())
    {
      thd_mark_transaction_to_rollback(ha_thd(), 1);
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (secondary_index_results_.empty())
    {
      return HA_ERR_END_OF_FILE;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  // Exact match search
  if (end_range == nullptr && !is_prefix_search && find_flag == HA_READ_KEY_EXACT)
  {
    auto serialized_key = convert_key_to_ldbformat(key, keypart_map);

    auto index_results = tx->read_secondary_index(current_index_name, serialized_key);

    for (auto &[ptr, size] : index_results)
    {
      std::string pk = std::string(reinterpret_cast<const char *>(ptr), size);
      secondary_index_results_.push_back(pk);
    }

    if (secondary_index_results_.empty())
    {
      return HA_ERR_KEY_NOT_FOUND;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  // Range search (including prefix search)
  auto serialized_start_key = convert_key_to_ldbformat(key, keypart_map);
  std::string serialized_end_key;

  if (find_flag == HA_READ_AFTER_KEY)
  {
    // Exclude start key by appending a byte to search after it
    serialized_start_key.push_back('\x00');
    if (end_range != nullptr)
    {
      serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

      // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
      if (end_range->flag == HA_READ_BEFORE_KEY)
      {
        end_range_exclusive_key_ = serialized_end_key;
      }
    }
    else
    {
      serialized_end_key = std::string(serialized_start_key.size() + 1, '\xFF');
    }
  }
  else if (find_flag == HA_READ_KEY_OR_NEXT)
  {
    if (end_range != nullptr)
    {
      serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

      // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
      if (end_range->flag == HA_READ_BEFORE_KEY)
      {
        end_range_exclusive_key_ = serialized_end_key;
      }
    }
    else
    {
      serialized_end_key = std::string(serialized_start_key.size() + 1, '\xFF');
    }
  }
  else if (end_range != nullptr)
  {
    serialized_end_key = convert_key_to_ldbformat(end_range->key, end_range->keypart_map);

    // HA_READ_BEFORE_KEY means exclusive end boundary (< instead of <=)
    if (end_range->flag == HA_READ_BEFORE_KEY)
    {
      end_range_exclusive_key_ = serialized_end_key;
    }
    else
    {
      // Inclusive end: extend prefix to include all keys with this prefix
      uint end_used_key_parts = count_used_key_parts(key_info, end_range->keypart_map);
      if (end_used_key_parts < key_info->user_defined_key_parts)
      {
        serialized_end_key = build_prefix_range_end(serialized_end_key);
      }
    }
  }
  else
  {
    // Prefix search: generate end key by appending maximum values
    serialized_end_key = build_prefix_range_end(serialized_start_key);
  }

  // Only extend if not exclusive end - exclusive end should use original key as boundary
  if (serialized_end_key.size() < serialized_start_key.size() && end_range_exclusive_key_.empty())
  {
    serialized_end_key = build_prefix_range_end(serialized_end_key);
  }

  secondary_index_results_ = tx->get_matching_primary_keys_in_range(
      current_index_name, serialized_start_key, serialized_end_key, end_range_exclusive_key_);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (secondary_index_results_.empty())
  {
    return HA_ERR_KEY_NOT_FOUND;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief Serialize a single field value to LineairDB key format
 *
 * This helper function converts a MySQL Field to LineairDB's sortable key format
 * based on its type. This eliminates code duplication across different key handling
 * functions.
 *
 * @param field MySQL Field object
 * @return Serialized key string
 */
std::string ha_lineairdb::serialize_key_from_field(Field *field)
{
  const bool is_null = field->is_null();
  enum_field_types mysql_type = field->type();
  LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

  std::string payload;

  if (!is_null)
  {
    switch (ldb_type)
    {
    case LineairDBFieldType::LINEAIRDB_INT:
    {
      int64_t value = field->val_int();
      size_t field_len = field->pack_length();

      uchar buf[8] = {0};
      if (field_len == 1)
      {
        buf[0] = static_cast<uchar>(value & 0xFF);
      }
      else if (field_len == 2)
      {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
      }
      else if (field_len == 4)
      {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
        buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
        buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
      }
      else
      {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
        buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
        buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
        buf[4] = static_cast<uchar>((value >> 32) & 0xFF);
        buf[5] = static_cast<uchar>((value >> 40) & 0xFF);
        buf[6] = static_cast<uchar>((value >> 48) & 0xFF);
        buf[7] = static_cast<uchar>((value >> 56) & 0xFF);
        field_len = 8;
      }

      payload = encode_int_key(buf, field_len);
      break;
    }

    case LineairDBFieldType::LINEAIRDB_DATETIME:
    {
      size_t field_len = field->pack_length();
      std::string raw(field_len, '\0');
      field->get_key_image(reinterpret_cast<uchar *>(raw.data()), field_len,
                           Field::itRAW);
      payload = encode_datetime_key(reinterpret_cast<const uchar *>(raw.data()),
                                    field_len);
      break;
    }

    case LineairDBFieldType::LINEAIRDB_STRING:
    {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default:
    {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }
    }
  }

  std::string encoded;
  append_key_part_encoding(encoded, is_null, ldb_type, payload);
  return encoded;
}

std::string ha_lineairdb::build_secondary_key_from_row(
    const uchar *row_buffer,
    const KEY &key_info)
{
  // Temporarily set read_set to include all columns
  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);

  // Calculate the offset between row_buffer and record[0]
  ptrdiff_t offset = row_buffer - table->record[0];

  // Construct the secondary key
  std::string secondary_key;
  for (uint part_idx = 0; part_idx < key_info.user_defined_key_parts; part_idx++)
  {
    auto key_part = key_info.key_part[part_idx];
    Field *field = table->field[key_part.fieldnr - 1];

    // Adjust the Field pointer to match row_buffer
    field->move_field_offset(offset);

    // Serialize each key part and concatenate
    secondary_key += serialize_key_from_field(field);

    // Restore the Field pointer back to original position
    field->move_field_offset(-offset);
  }

  // Restore the original read_set
  tmp_restore_column_map(table->read_set, org_bitmap);

  return secondary_key;
}

void ha_lineairdb::store_primary_key_in_ref(const std::string &primary_key)
{
  if (table == nullptr || table->s == nullptr || ref == nullptr)
  {
    return;
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t))
  {
    return;
  }

  if (primary_key.size() > std::numeric_limits<uint16_t>::max())
  {
    std::cerr << "[LineairDB][position] primary key length exceeds uint16_t: "
              << primary_key.size() << std::endl;
    return;
  }

  const size_t payload_capacity = ref_length_local - sizeof(uint16_t);
  if (primary_key.size() > payload_capacity)
  {
    std::cerr << "[LineairDB][position] primary key length exceeds ref capacity: "
              << primary_key.size() << " > " << payload_capacity << std::endl;
    return;
  }

  const uint16_t key_length = static_cast<uint16_t>(primary_key.size());
  std::memcpy(ref, &key_length, sizeof(uint16_t));

  if (key_length > 0)
  {
    std::memcpy(ref + sizeof(uint16_t), primary_key.data(), key_length);
  }

  const size_t remaining = payload_capacity - key_length;
  if (remaining > 0)
  {
    std::memset(ref + sizeof(uint16_t) + key_length, 0, remaining);
  }
}

std::string ha_lineairdb::extract_primary_key_from_ref(const uchar *pos) const
{
  if (pos == nullptr || table == nullptr || table->s == nullptr)
  {
    return {};
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t))
  {
    return {};
  }

  uint16_t key_length = 0;
  std::memcpy(&key_length, pos, sizeof(uint16_t));

  if (key_length == 0)
  {
    return {};
  }

  if (sizeof(uint16_t) + key_length > ref_length_local)
  {
    return {};
  }

  std::string key(reinterpret_cast<const char *>(pos + sizeof(uint16_t)),
                  key_length);

  return key;
}

bool ha_lineairdb::uses_hidden_primary_key() const
{
  if (table == nullptr || table->s == nullptr)
  {
    return false;
  }
  return table->s->primary_key == MAX_KEY;
}

std::string ha_lineairdb::serialize_hidden_primary_key(uint64_t row_id) const
{
  std::ostringstream oss;
  oss << std::hex << std::setw(16) << std::setfill('0') << row_id;
  return oss.str();
}

std::string ha_lineairdb::generate_hidden_primary_key()
{
  if (share == nullptr)
  {
    share = get_share();
  }
  uint64_t row_id = share->next_hidden_pk.fetch_add(1, std::memory_order_relaxed);
  std::string key = serialize_hidden_primary_key(row_id);
  return key;
}

std::string ha_lineairdb::extract_key(const uchar *buf)
{
  if (is_primary_key_exists())
  {
    return extract_key_from_mysql(buf);
  }
  else
  {
    return autogenerate_key();
  }
}

std::string ha_lineairdb::extract_key_from_mysql(const uchar *row_buffer)
{
  std::string complete_key;

  // Guard: return empty if no explicit primary key exists
  if (!is_primary_key_exists() || key_part == nullptr || num_key_parts == 0)
  {
    return complete_key;
  }

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  ptrdiff_t offset = row_buffer - table->record[0];

  for (size_t i = 0; i < num_key_parts; ++i)
  {
    auto field_index = key_part[i].fieldnr - 1;
    Field *field = table->field[field_index];

    field->move_field_offset(offset);
    complete_key += serialize_key_from_field(field);
    field->move_field_offset(-offset);
  }

  tmp_restore_column_map(table->read_set, org_bitmap);

  return complete_key;
}

std::string ha_lineairdb::autogenerate_key()
{
  return generate_hidden_primary_key();
}

/**
 * @brief Encode INT key from MySQL format to LineairDB sortable format
 *
 * Converts little-endian integer to big-endian with sign bit flipped.
 * This ensures correct lexicographic ordering: negative < 0 < positive
 *
 * @param data MySQL key data (little-endian)
 * @param len Key length (1, 2, 4, or 8 bytes)
 * @return Big-endian binary string with sign bit flipped
 */
std::string ha_lineairdb::encode_int_key(const uchar *data, size_t len)
{
  uint64_t value = 0;

  if (len == 1)
  {
    value = static_cast<uint8_t>(data[0]);
  }
  else if (len == 2)
  {
    value = static_cast<uint16_t>(data[0]) |
            (static_cast<uint16_t>(data[1]) << 8);
  }
  else if (len == 4)
  {
    value = static_cast<uint32_t>(data[0]) |
            (static_cast<uint32_t>(data[1]) << 8) |
            (static_cast<uint32_t>(data[2]) << 16) |
            (static_cast<uint32_t>(data[3]) << 24);
  }
  else if (len == 8)
  {
    value = static_cast<uint64_t>(data[0]) |
            (static_cast<uint64_t>(data[1]) << 8) |
            (static_cast<uint64_t>(data[2]) << 16) |
            (static_cast<uint64_t>(data[3]) << 24) |
            (static_cast<uint64_t>(data[4]) << 32) |
            (static_cast<uint64_t>(data[5]) << 40) |
            (static_cast<uint64_t>(data[6]) << 48) |
            (static_cast<uint64_t>(data[7]) << 56);
  }
  else
  {
    // Unsupported length
    return std::string();
  }

  // Flip sign bit for correct sorting
  // This makes: negative numbers < 0 < positive numbers
  if (len == 1)
  {
    value ^= 0x80ULL;
  }
  else if (len == 2)
  {
    value ^= 0x8000ULL;
  }
  else if (len == 4)
  {
    value ^= 0x80000000ULL;
  }
  else if (len == 8)
  {
    value ^= 0x8000000000000000ULL;
  }

  // Convert to big-endian
  char buf[8];
  size_t output_len = len;
  for (size_t i = 0; i < output_len; i++)
  {
    buf[i] = static_cast<char>((value >> ((output_len - 1 - i) * 8)) & 0xFF);
  }

  return std::string(buf, output_len);
}

/**
 * @brief Encode DATETIME key from MySQL format to LineairDB format
 *
 * MySQL DATETIME is already stored in a sortable binary format,
 * so we just copy it as-is.
 *
 * @param data MySQL DATETIME binary data
 * @param len Key length (typically 5 or 8 bytes)
 * @return Binary string (unchanged)
 */
std::string ha_lineairdb::encode_datetime_key(const uchar *data, size_t len)
{
  // MySQL DATETIME2 is already in sortable format, just copy it
  return std::string(reinterpret_cast<const char *>(data), len);
}

/**
 * @brief Encode VARCHAR key from MySQL format to LineairDB format
 *
 * MySQL stores VARCHAR keys with a 2-byte length prefix (little-endian).
 * We extract the actual string data without padding.
 *
 * @param data MySQL VARCHAR key data (length prefix + string + padding)
 * @param len Total key length
 * @return Actual string data without prefix or padding
 */
std::string ha_lineairdb::encode_string_key(const uchar *data, size_t len)
{
  if (len < 2)
    return std::string();

  // First 2 bytes are length (little-endian)
  uint16_t str_len = static_cast<uint16_t>(data[0]) |
                     (static_cast<uint16_t>(data[1]) << 8);

  if (str_len == 0 || len < 2 + str_len)
  {
    // Invalid or empty string
    return std::string();
  }

  // Return actual string data (skip 2-byte prefix, exclude padding)
  return std::string(reinterpret_cast<const char *>(data + 2), str_len);
}

/**
 * @brief Convert MySQL binary composite key format to LineairDB sortable key format
 *
 * This function handles composite keys by processing each key part sequentially:
 * - Reads key_part_map to determine which parts are used
 * - Converts each part to sortable format based on its type
 * - Concatenates all parts into a single sortable string
 *
 * Key formats by type:
 * - INT: Little-endian to big-endian + sign bit flip (for correct sorting)
 * - DATETIME: Pass through as-is (already sortable)
 * - STRING (VARCHAR): Extract actual data (remove length prefix and padding)
 *
 * @param key MySQL binary key data (concatenated byte array)
 * @param keypart_map Bitmap indicating which key parts are used
 * @return LineairDB formatted key string (concatenated sortable format)
 */
std::string ha_lineairdb::convert_key_to_ldbformat(const uchar *key, key_part_map keypart_map)
{
  KEY *key_info = &table->key_info[active_index];
  std::string result;
  const uchar *key_ptr = key;

  // Process each key part sequentially
  for (uint i = 0; i < key_info->user_defined_key_parts; i++)
  {
    // Check if this key part is used in the query
    if (!((keypart_map >> i) & 1))
    {
      break; // Remaining parts are not used (prefix scan)
    }

    KEY_PART_INFO *kp = &key_info->key_part[i];
    Field *field = kp->field;
    bool is_null = false;
    if (kp->null_bit)
    {
      is_null = (*key_ptr != 0);
      key_ptr++; // Skip NULL flag byte

      if (is_null)
      {
        key_ptr += (kp->store_length - 1);
        append_key_part_encoding(result, true,
                                 convert_mysql_type_to_lineairdb(field->type()),
                                 std::string());
        continue;
      }
    }

    uint data_len = kp->length;
    const uchar *data_ptr = key_ptr;

    if (kp->key_part_flag & HA_VAR_LENGTH_PART)
    {
      data_len = uint2korr(data_ptr);
      data_ptr += 2; // Skip length prefix
      key_ptr = data_ptr;
    }

    enum_field_types mysql_type = field->type();
    LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

    std::string payload;
    switch (ldb_type)
    {
    case LineairDBFieldType::LINEAIRDB_INT:
      payload = encode_int_key(data_ptr, data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_DATETIME:
      payload = encode_datetime_key(data_ptr, data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_STRING:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;
    }

    append_key_part_encoding(result, false, ldb_type, payload);

    if (kp->key_part_flag & HA_VAR_LENGTH_PART)
    {
      key_ptr += kp->length;
    }
    else
    {
      key_ptr += kp->length;
    }
  }

  return result;
}

/**
 * @brief This function only extracts the type of key for
 *        tables that have single key
 *
 * @return bytes Key type is int
 * @return 0 Key type is not int
 */
bool ha_lineairdb::is_primary_key_type_int()
{
  ha_base_keytype integer_types[] = {
      HA_KEYTYPE_SHORT_INT, HA_KEYTYPE_USHORT_INT, HA_KEYTYPE_LONG_INT,
      HA_KEYTYPE_ULONG_INT, HA_KEYTYPE_LONGLONG, HA_KEYTYPE_ULONGLONG,
      HA_KEYTYPE_INT24, HA_KEYTYPE_UINT24, HA_KEYTYPE_INT8};
  assert(table->s->keys == 1);
  ha_base_keytype key_type = primary_key_type;
  return std::find(std::begin(integer_types), std::end(integer_types),
                   key_type) != std::end(integer_types);
}

/**
 * @brief Format and set the requested row into `write_buffer_`.
 */
void ha_lineairdb::set_write_buffer(uchar *buf)
{
  ldbField.set_null_field(buf, table->s->null_bytes);
  write_buffer_ = ldbField.get_null_field();

  char attribute_buffer[1024];
  String attribute(attribute_buffer, sizeof(attribute_buffer), &my_charset_bin);

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  for (Field **field = table->field; *field; field++)
  {
    if ((*field)->is_nullable() && (*field)->is_null())
    {
      ldbField.set_lineairdb_field("", 0);
    }
    else
    {
      attribute.length(0);
      (*field)->val_str(&attribute, &attribute);
      ldbField.set_lineairdb_field(attribute.c_ptr(), attribute.length());
    }
    write_buffer_ += ldbField.get_lineairdb_field();
  }
  tmp_restore_column_map(table->read_set, org_bitmap);
}

bool ha_lineairdb::is_primary_key_exists()
{
  return table->s->primary_key != MAX_KEY;
}

bool ha_lineairdb::store_blob_to_field(Field **field)
{
  if ((*field)->is_flag_set(BLOB_FLAG))
  {
    Field_blob *blob_field = down_cast<Field_blob *>(*field);
    size_t length = blob_field->get_length();
    if (length > 0)
    {
      unsigned char *new_blob = new (&blobroot) unsigned char[length];
      if (new_blob == nullptr)
        return true;
      memcpy(new_blob, blob_field->get_blob_data(), length);
      blob_field->set_ptr(length, new_blob);
    }
  }
  return false;
}

int ha_lineairdb::set_fields_from_lineairdb(uchar *buf,
                                            const std::byte *const read_buf,
                                            const size_t read_buf_size)
{
  // Clear BLOB data from the previous row.
  blobroot.ClearForReuse();
  ldbField.make_mysql_table_row(read_buf, read_buf_size);
  /**
   * for each 8 potentially null columns, buf holds 1 byte flag at the front
   * the number of null flag bytes in buf is shown in table->s->null_bytes
   * the flag is originally set to 0xff, or b11111111
   * if you want to make the first potentially null column to show a non-null
   * value, store 0xfe, or b11111110, in buf
   */
  auto nullFlags = ldbField.get_null_flags();
  for (size_t i = 0; i < nullFlags.size(); i++)
  {
    buf[i] = nullFlags[i];
  }

  /* Avoid asserts in ::store() for columns that are not going to be updated
   */
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
  /**
   * store each column value to corresponding field
   */
  size_t columnIndex = 0;
  for (Field **field = table->field; *field; field++)
  {
    const auto mysqlFieldValue = ldbField.get_column_of_row(columnIndex++);
    if ((*field)->is_nullable() && (*field)->is_null_in_record(buf))
    {
      (*field)->set_null();
    }
    else
    {
      (*field)->store(mysqlFieldValue.c_str(), mysqlFieldValue.length(),
                      &my_charset_bin, CHECK_FIELD_WARN);
      if (store_blob_to_field(field))
        return HA_ERR_OUT_OF_MEM;
    }
  }
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  return 0;
}

struct st_mysql_storage_engine lineairdb_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

static ulong srv_enum_var = 0;
static ulong srv_ulong_var = 0;
static double srv_double_var = 0;
static int srv_signed_int_var = 0;
static long srv_signed_long_var = 0;
static longlong srv_signed_longlong_var = 0;

const char *enum_var_names[] = {"e1", "e2", NullS};

TYPELIB enum_var_typelib = {array_elements(enum_var_names) - 1,
                            "enum_var_typelib", enum_var_names, nullptr};

static MYSQL_SYSVAR_ENUM(enum_var,                       // name
                         srv_enum_var,                   // varname
                         PLUGIN_VAR_RQCMDARG,            // opt
                         "Sample ENUM system variable.", // comment
                         nullptr,                        // check
                         nullptr,                        // update
                         0,                              // def
                         &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
                          "0..1000", nullptr, nullptr, 8, 0, 1000, 0);

static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5,
                           0); // reserved always 0

static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5, 0);

static MYSQL_SYSVAR_INT(signed_int_var, srv_signed_int_var, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_THDVAR_INT(signed_int_thdvar, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_SYSVAR_LONG(signed_long_var, srv_signed_long_var,
                         PLUGIN_VAR_RQCMDARG, "LONG_MIN..LONG_MAX", nullptr,
                         nullptr, -10, LONG_MIN, LONG_MAX, 0);

static MYSQL_THDVAR_LONG(signed_long_thdvar, PLUGIN_VAR_RQCMDARG,
                         "LONG_MIN..LONG_MAX", nullptr, nullptr, -10, LONG_MIN,
                         LONG_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(signed_longlong_var, srv_signed_longlong_var,
                             PLUGIN_VAR_RQCMDARG, "LLONG_MIN..LLONG_MAX",
                             nullptr, nullptr, -10, LLONG_MIN, LLONG_MAX, 0);

static MYSQL_THDVAR_LONGLONG(signed_longlong_thdvar, PLUGIN_VAR_RQCMDARG,
                             "LLONG_MIN..LLONG_MAX", nullptr, nullptr, -10,
                             LLONG_MIN, LLONG_MAX, 0);

// TPC-C mode: Enable hardcoded statistics for TPC-C benchmark optimization
// (srv_tpcc_mode is declared near the top of this file)
static MYSQL_SYSVAR_BOOL(tpcc_mode, srv_tpcc_mode,
                         PLUGIN_VAR_RQCMDARG,
                         "Enable TPC-C benchmark mode with hardcoded statistics. "
                         "When ON, optimizer statistics are tuned for TPC-C tables. "
                         "Default: OFF",
                         nullptr, nullptr, false);

// TPC-C warehouses: Number of warehouses for row count estimation
// (srv_tpcc_warehouses is declared near the top of this file)
static MYSQL_SYSVAR_ULONG(tpcc_warehouses, srv_tpcc_warehouses,
                          PLUGIN_VAR_RQCMDARG,
                          "Number of TPC-C warehouses for statistics estimation. "
                          "Used to calculate expected row counts. Default: 1",
                          nullptr, nullptr, 1, 1, 10000, 0);

static SYS_VAR *lineairdb_system_variables[] = {
    MYSQL_SYSVAR(enum_var),
    MYSQL_SYSVAR(ulong_var),
    MYSQL_SYSVAR(double_var),
    MYSQL_SYSVAR(double_thdvar),
    MYSQL_SYSVAR(last_create_thdvar),
    MYSQL_SYSVAR(create_count_thdvar),
    MYSQL_SYSVAR(signed_int_var),
    MYSQL_SYSVAR(signed_int_thdvar),
    MYSQL_SYSVAR(signed_long_var),
    MYSQL_SYSVAR(signed_long_thdvar),
    MYSQL_SYSVAR(signed_longlong_var),
    MYSQL_SYSVAR(signed_longlong_thdvar),
    MYSQL_SYSVAR(tpcc_mode),
    MYSQL_SYSVAR(tpcc_warehouses),
    nullptr};

// this is an lineairdb of SHOW_FUNC
static int show_func_lineairdb(MYSQL_THD, SHOW_VAR *var, char *buf)
{
  var->type = SHOW_CHAR;
  var->value = buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
           "enum_var is %lu, ulong_var is %lu, "
           "double_var is %f, signed_int_var is %d, "
           "signed_long_var is %ld, signed_longlong_var is %lld",
           srv_enum_var, srv_ulong_var, srv_double_var, srv_signed_int_var,
           srv_signed_long_var, srv_signed_longlong_var);
  return 0;
}

lineairdb_vars_t lineairdb_vars = {100, 20.01, "three hundred",
                                   true, false, 8250};

static SHOW_VAR show_status_lineairdb[] = {
    {"var1", (char *)&lineairdb_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"var2", (char *)&lineairdb_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF,
     SHOW_SCOPE_UNDEF} // null terminator required
};

static SHOW_VAR show_array_lineairdb[] = {
    {"array", (char *)show_status_lineairdb, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
    {"var3", (char *)&lineairdb_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
    {"var4", (char *)&lineairdb_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

static SHOW_VAR func_status[] = {
    {"lineairdb_func_lineairdb", (char *)show_func_lineairdb, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {"lineairdb_status_var5", (char *)&lineairdb_vars.var5, SHOW_BOOL,
     SHOW_SCOPE_GLOBAL},
    {"lineairdb_status_var6", (char *)&lineairdb_vars.var6, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"lineairdb_status", (char *)show_array_lineairdb, SHOW_ARRAY,
     SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

mysql_declare_plugin(lineairdb){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &lineairdb_storage_engine,
    "LINEAIRDB",
    PLUGIN_AUTHOR_ORACLE,
    "LineairDB storage engine",
    PLUGIN_LICENSE_GPL,
    lineairdb_init_func, /* Plugin Init */
    nullptr,             /* Plugin check uninstall */
    nullptr,             /* Plugin Deinit */
    0x0001 /* 0.1 */,
    func_status,                /* status variables */
    lineairdb_system_variables, /* system variables */
    nullptr,                    /* config options */
    0,                          /* flags */
} mysql_declare_plugin_end;