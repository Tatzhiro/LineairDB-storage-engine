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

#include <iostream>
#include <fstream>
#include <iomanip>

#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"
#include "storage/innobase/include/dict0mem.h"
#include "lineairdb_field_types.h"

#define BLOB_MEMROOT_ALLOC_SIZE (8192)
#define FENCE true

static std::shared_ptr<LineairDB::Database> get_or_allocate_database(
    LineairDB::Config conf);

void terminate_tx(LineairDBTransaction *&tx);
static int lineairdb_commit(handlerton *hton, THD *thd, bool shouldCommit);
static int lineairdb_abort(handlerton *hton, THD *thd, bool);

static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, nullptr,
                        nullptr, nullptr, nullptr);

static MYSQL_THDVAR_UINT(create_count_thdvar, 0, nullptr, nullptr, nullptr, 0,
                         0, 1000, 0);

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
      blobroot(csv_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {}

void ha_lineairdb::set_key_and_key_part_info(const TABLE *const table)
{
  key_info = table->key_info;
  uint pk_index = table->s->primary_key;

  // 主キーが存在する場合のみ情報を設定
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
    // 主キーがない場合はデフォルト値を設定
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
  fprintf(stderr, "[DEBUG] db_table_name = %s\n", db_table_name.c_str());

  if ((num_keys = table->s->keys))
    set_key_and_key_part_info(table);

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
  return change_active_index(idx);
}

int ha_lineairdb::index_end()
{
  DBUG_TRACE;
  active_index = MAX_KEY;
  return 0;
}

int ha_lineairdb::index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag)
{
  DBUG_TRACE;
  return 0;
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

  auto key = extract_key();
  set_write_buffer(buf);

  auto tx = get_transaction(userThread);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  bool is_successful = tx->write(key, write_buffer_);
  if (!is_successful)
    return HA_ERR_LOCK_DEADLOCK;

  for (uint i = 0; i < table->s->keys; i++)
  {
    auto key_info = table->key_info[i];
    if (i != table->s->primary_key)
    {
      my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);

      // Support composite indexes: process all key parts
      std::string secondary_key;
      for (uint part_idx = 0; part_idx < key_info.user_defined_key_parts; part_idx++)
      {
        auto key_part = key_info.key_part[part_idx];
        Field *field = table->field[key_part.fieldnr - 1];

        // Encode each key part and concatenate
        secondary_key += serialize_key_from_field(field);
      }

      tmp_restore_column_map(table->read_set, org_bitmap);

      bool is_successful = tx->write_secondary_index(key_info.name, secondary_key, key);
      if (!is_successful)
        return HA_ERR_LOCK_DEADLOCK;
    }
  }

  return 0;
}

int ha_lineairdb::update_row(const uchar *, uchar *buf)
{
  DBUG_TRACE;

  auto key = extract_key();
  set_write_buffer(buf);

  auto tx = get_transaction(userThread);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  bool is_successful = tx->write(key, write_buffer_);
  if (!is_successful)
    return HA_ERR_LOCK_DEADLOCK;

  return 0;
}

int ha_lineairdb::delete_row(const uchar *)
{
  DBUG_TRACE;

  auto key = extract_key();

  auto tx = get_transaction(userThread);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  bool is_successful = tx->delete_value(key);
  if (!is_successful)
    return HA_ERR_LOCK_DEADLOCK;

  return 0;
}

int ha_lineairdb::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                                 enum ha_rkey_function)
{
  DBUG_TRACE;

  stats.records = 0;
  auto tx = get_transaction(userThread);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  secondary_index_results_.clear();
  current_position_in_index_ = 0;

  // Check if this is a prefix search (not all key parts are specified)
  KEY *key_info = &table->key_info[active_index];
  uint used_key_parts = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++)
  {
    if ((keypart_map >> i) & 1)
      used_key_parts++;
    else
      break;
  }
  bool is_prefix_search = (used_key_parts < key_info->user_defined_key_parts);

  // ===== PRIMARY KEY処理 =====
  if (active_index == table->s->primary_key)
  {
    auto serialized_key = convert_key_to_ldbformat(key, keypart_map);

    if (end_range == nullptr && !is_prefix_search)
    {
      // PRIMARY KEY完全一致: 直接read()を使用
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

      // index_next()が呼ばれる可能性があるため、結果を保存
      secondary_index_results_.push_back(serialized_key);
      current_position_in_index_ = 1; // 既に1件読み取り済み

      return 0;
    }
    else
    {
      // PRIMARY KEY前方一致/範囲検索
      std::string serialized_end_key;

      if (end_range != nullptr)
      {
        // 明示的なend_rangeが提供されている場合
        serialized_end_key = convert_key_to_ldbformat(end_range->key, keypart_map);
      }
      else
      {
        // プレフィックス検索: 終了キーを生成
        serialized_end_key = serialized_key + std::string(256, '\xFF');
      }

      // 範囲検索用のget_matching_keys_in_range()を使用
      secondary_index_results_ = tx->get_matching_keys_in_range(
          serialized_key, serialized_end_key);

      if (secondary_index_results_.empty())
      {
        return HA_ERR_KEY_NOT_FOUND;
      }

      std::string primary_key = secondary_index_results_[current_position_in_index_];
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
      return 0;
    }
  }

  // ===== SECONDARY INDEX処理（既存のコード） =====
  if (end_range == nullptr && !is_prefix_search)
  {
    // Exact match search: all key parts are specified
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

    std::string primary_key = secondary_index_results_[current_position_in_index_];
    auto result = tx->read(primary_key);

    if (set_fields_from_lineairdb(buf, result.first, result.second))
    {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }
    current_position_in_index_++;
    return 0;
  }
  else
  {
    // Range search (including prefix search)
    auto serialized_start_key = convert_key_to_ldbformat(key, keypart_map);
    std::string serialized_end_key;

    if (end_range != nullptr)
    {
      // Explicit end range provided
      serialized_end_key = convert_key_to_ldbformat(end_range->key, keypart_map);
    }
    else
    {
      // Prefix search: generate end key by appending maximum values
      // This will match all keys that start with serialized_start_key
      serialized_end_key = serialized_start_key + std::string(256, '\xFF');
    }

    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, serialized_start_key, serialized_end_key);

    if (secondary_index_results_.empty())
    {
      return HA_ERR_KEY_NOT_FOUND;
    }

    std::string primary_key = secondary_index_results_[current_position_in_index_];
    auto result = tx->read(primary_key);

    if (set_fields_from_lineairdb(buf, result.first, result.second))
    {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }
    current_position_in_index_++;
    return 0;
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

  auto tx = get_transaction(userThread);
  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }
  std::string primary_key = secondary_index_results_[current_position_in_index_];
  auto result = tx->read(primary_key);
  if (set_fields_from_lineairdb(buf, result.first, result.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }
  current_position_in_index_++;
  return 0;
}

int ha_lineairdb::index_next_same(uchar *buf, const uchar *key, uint key_len)
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

  auto tx = get_transaction(userThread);
  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }
  std::string primary_key = secondary_index_results_[current_position_in_index_];
  auto result = tx->read(primary_key);
  if (set_fields_from_lineairdb(buf, result.first, result.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }
  current_position_in_index_++;
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
  int rc;
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
  current_position_ = 0;
  stats.records = 0;

  auto tx = get_transaction(userThread);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  tx->choose_table(db_table_name);
  scanned_keys_ = tx->get_all_keys();

  DBUG_RETURN(0);
}

int ha_lineairdb::rnd_end()
{
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
int ha_lineairdb::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_lineairdb::rnd_next");
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  if (scanned_keys_.size() == 0)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

read_from_lineairdb:
  if (current_position_ == scanned_keys_.size())
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  auto &key = scanned_keys_[current_position_];

  auto tx = get_transaction(userThread);

  if (tx->is_aborted())
  {
    thd_mark_transaction_to_rollback(userThread, 1);
    return HA_ERR_LOCK_DEADLOCK;
  }

  assert(tx->get_selected_table_name() == db_table_name);
  auto read_buffer = tx->read(key);

  if (read_buffer.first == nullptr)
  {
    current_position_++;
    goto read_from_lineairdb;
  }
  if (set_fields_from_lineairdb(buf, read_buffer.first, read_buffer.second))
  {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }
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
void ha_lineairdb::position(const uchar *) { DBUG_TRACE; }

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
int ha_lineairdb::rnd_pos(uchar *, uchar *)
{
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
int ha_lineairdb::info(uint)
{
  DBUG_TRACE;
  /* This is a lie, but you don't want the optimizer to see zero or 1 */
  if (stats.records < 2)
    stats.records = 2;
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

  userThread = thd;
  LineairDBTransaction *&tx = get_transaction(thd);

  const bool tx_is_ready_to_commit = lock_type == F_UNLCK;
  if (tx_is_ready_to_commit)
  {
    if (tx->is_a_single_statement())
    {
      lineairdb_commit(lineairdb_hton, thd, true);
    }
    return 0;
  }

  if (tx->is_not_started())
  {
    tx->begin_transaction();
  }

  return 0;
}

int ha_lineairdb::start_stmt(THD *thd, thr_lock_type lock_type)
{
  assert(lock_type > 0);
  return external_lock(thd, lock_type);
}

/**
 * @brief Gets transaction from MySQL allocated memory
 */
LineairDBTransaction *&ha_lineairdb::get_transaction(THD *thd)
{
  LineairDBTransaction *&tx = *reinterpret_cast<LineairDBTransaction **>(
      thd_ha_data(thd, lineairdb_hton));
  if (tx == nullptr)
  {
    tx = new LineairDBTransaction(thd, get_db(), lineairdb_hton, FENCE);
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
THR_LOCK_DATA **ha_lineairdb::store_lock(THD *, THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
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
ha_rows ha_lineairdb::records_in_range(uint, key_range *, key_range *)
{
  DBUG_TRACE;
  return 10; // low number to force index usage
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
  fprintf(stderr, "[DEBUG] db_table_name = %s\n", db_table_name.c_str());
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

ha_rows ha_lineairdb::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                                  void *seq_init_param, uint n_ranges,
                                                  uint *bufsz, uint *flags,
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
  enum_field_types mysql_type = field->type();
  LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

  switch (ldb_type)
  {
  case LineairDBFieldType::LINEAIRDB_INT:
  {
    // Get integer value and convert to MySQL binary format first
    int64_t value = field->val_int();
    size_t field_len = field->pack_length();

    // Convert to little-endian (MySQL format)
    uchar buf[8];
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
    { // 8 bytes
      buf[0] = static_cast<uchar>(value & 0xFF);
      buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
      buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
      buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
      buf[4] = static_cast<uchar>((value >> 32) & 0xFF);
      buf[5] = static_cast<uchar>((value >> 40) & 0xFF);
      buf[6] = static_cast<uchar>((value >> 48) & 0xFF);
      buf[7] = static_cast<uchar>((value >> 56) & 0xFF);
    }
    // Use the same encoder as convert_key_to_ldbformat
    std::string result = encode_int_key(buf, field_len);

    return result;
  }

  case LineairDBFieldType::LINEAIRDB_DATETIME:
  {
    // For DATETIME, get the packed binary representation
    // MySQL stores DATETIME in a packed format that is already sortable
    size_t field_len = field->pack_length();
    uchar buf[8];

    // Pack the datetime value
    field->get_key_image(buf, field_len, Field::itRAW);

    // Use the datetime encoder (which just copies it)
    std::string result = encode_datetime_key(buf, field_len);

    return result;
  }

  case LineairDBFieldType::LINEAIRDB_STRING:
  {
    // For string types, use as-is (no conversion needed)
    String buffer;
    field->val_str(&buffer, &buffer);

    return std::string(buffer.c_ptr(), buffer.length());
  }

  case LineairDBFieldType::LINEAIRDB_OTHER:
  default:
  {
    // For unsupported types, treat as string
    String buffer;
    field->val_str(&buffer, &buffer);

    return std::string(buffer.c_ptr(), buffer.length());
  }
  }
}

std::string ha_lineairdb::extract_key()
{
  if (is_primary_key_exists())
  {
    return get_key_from_mysql();
  }
  else
  {
    return autogenerate_key();
  }
}

std::string ha_lineairdb::get_key_from_mysql()
{
  std::string complete_key;

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  assert((*(table->field + indexed_key_part.fieldnr - 1))->key_start.is_set(0));

  for (size_t i = 0; i < num_key_parts; i++)
  {
    auto field_index = key_part[i].fieldnr - 1;
    auto key_part_field = table->field[field_index];

    // Convert key part using the helper function
    complete_key += serialize_key_from_field(key_part_field);
  }

  tmp_restore_column_map(table->read_set, org_bitmap);

  return complete_key;
}

std::string ha_lineairdb::autogenerate_key()
{
  /**
   * @WANTFIX: This function relies on a class member `auto_generated_keys_`.
   * `auto_generated_keys_` is not recovered when the handler is constructed.
   * It has to be a recoverable data.
   */
  std::cout << "ha_lineairdb::autogenerate_key NEEDS FIX" << std::endl;
  std::string generated_key;
  auto inserted_count = auto_generated_keys_[db_table_name]++;
  generated_key = std::to_string(inserted_count);
  return generated_key;
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

  // Read little-endian integer (支持1/2/4/8字节)
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

    // Step 1: Handle NULL flag if column is nullable
    if (kp->null_bit)
    {
      bool is_null = (*key_ptr != 0);
      key_ptr++; // Skip NULL flag byte

      if (is_null)
      {
        // Encode NULL as maximum value (sorts last in SQL)
        // Use the field's data length for consistent sizing
        result += std::string(kp->length, '\xFF');
        // Skip remaining bytes for this part
        key_ptr += (kp->store_length - 1);
        continue;
      }
    }

    // Step 2: Handle variable-length fields (VARCHAR, TEXT, BLOB)
    uint data_len = kp->length;
    if (kp->key_part_flag & HA_VAR_LENGTH_PART)
    {
      // Read 2-byte little-endian length prefix
      data_len = uint2korr(key_ptr);
      key_ptr += 2;
    }

    // Step 3: Encode data based on field type
    enum_field_types mysql_type = field->type();
    LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

    switch (ldb_type)
    {
    case LineairDBFieldType::LINEAIRDB_INT:
      // For variable-length key parts, use actual data_len
      // For fixed-length, data_len equals kp->length
      result += encode_int_key(key_ptr, data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_DATETIME:
      result += encode_datetime_key(key_ptr, data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_STRING:
      // String data is already in sortable format (lexicographic order)
      // Just copy the actual data without length prefix or padding
      result += std::string(reinterpret_cast<const char *>(key_ptr), data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default:
      // Unknown types: treat as raw binary data
      result += std::string(reinterpret_cast<const char *>(key_ptr), data_len);
      break;
    }

    // Step 4: Move pointer to next key part
    // For variable-length fields, skip the remaining padding
    if (kp->key_part_flag & HA_VAR_LENGTH_PART)
    {
      // We already moved past the 2-byte length prefix
      // Now skip the data and any padding
      key_ptr += kp->length;
    }
    else
    {
      // For fixed-length fields, just skip the data
      key_ptr += data_len;
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
    (*field)->val_str(&attribute, &attribute);
    ldbField.set_lineairdb_field(attribute.c_ptr(), attribute.length());
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
    (*field)->store(mysqlFieldValue.c_str(), mysqlFieldValue.length(),
                    &my_charset_bin, CHECK_FIELD_WARN);
    if (store_blob_to_field(field))
      return HA_ERR_OUT_OF_MEM;
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