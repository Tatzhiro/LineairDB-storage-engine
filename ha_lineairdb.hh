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

/** @file ha_lineairdb.h

    @brief
  The ha_lineairdb engine is a stubbed storage engine for lineairdb purposes
  only; it does nothing at this point. Its purpose is to provide a source code
  illustration of how to begin writing new storage engines; see also
  /storage/lineairdb/ha_lineairdb.cc.

    @note
  Please read ha_lineairdb.cc before reading this file.
  Reminder: The lineairdb storage engine implements all methods that are
  *required* to be implemented. For a full list of all methods that you can
  implement, see handler.h.

   @see
  /sql/handler.h and /storage/lineairdb/ha_lineairdb.cc
*/

#ifndef HA_LINEAIRDB_H
#define HA_LINEAIRDB_H

#include <lineairdb/lineairdb.h>
#include <string.h>
#include <sys/types.h>

#include <atomic>
#include <vector>

#include "lineairdb_field_types.h"
#include "lineairdb_field.hh"
#include "lineairdb_transaction.hh"
#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "sql/handler.h" /* handler */
#include "sql_string.h"
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */

/** @brief
  LineairDB_share is a class that will be shared among all open handlers.
  This lineairdb implements the minimum of what you will probably need.
*/
class LineairDB_share : public Handler_share
{
public:
  THR_LOCK lock;
  LineairDB_share();
  // std::shared_ptr<LineairDB::Database>
  // get_or_allocate_database(LineairDB::Config conf);
  ~LineairDB_share() override { thr_lock_delete(&lock); }
  std::shared_ptr<LineairDB::Database> lineairdb_;
  std::atomic<uint64_t> next_hidden_pk{0};
};

/** @brief
  Class definition for the storage engine
*/
class ha_lineairdb : public handler
{
  THR_LOCK_DATA lock;           ///< MySQL lock
  LineairDB_share *share;       ///< Shared lock info
  LineairDB_share *get_share(); ///< Get the share
  LineairDB::Database *get_db();

private:
  std::string db_table_name;
  std::string current_index_name;

  KEY *key_info;
  size_t num_keys;
  ha_base_keytype primary_key_type;

  KEY_PART_INFO *key_part;
  size_t num_key_parts;
  KEY_PART_INFO indexed_key_part;

  // Note: Previously had THD *userThread; but it was replaced by ha_thd()
  // which provides reliable access to the current thread handle.
  uint current_position_in_index_;
  std::vector<std::string> scanned_keys_;
  std::vector<std::string> secondary_index_results_;
  std::string last_fetched_primary_key_;
  std::string end_range_exclusive_key_; // For HA_READ_BEFORE_KEY: exclude this key from results
  my_off_t
      current_position_; /* Current position in the file during a file scan */
  std::string write_buffer_;
  LineairDBField ldbField;
  MEM_ROOT blobroot;

  // バッファフェッチ用の状態
  static constexpr size_t SCAN_BATCH_SIZE = 100;
  size_t buffer_position_{0};
  std::string last_batch_key_;
  bool scan_exhausted_{false};

  void store_primary_key_in_ref(const std::string &primary_key);
  std::string extract_primary_key_from_ref(const uchar *pos) const;
  bool uses_hidden_primary_key() const;
  std::string generate_hidden_primary_key();
  std::string serialize_hidden_primary_key(uint64_t row_id) const;
  std::string format_row_debug(const uchar *row_buffer) const;
  bool fetch_next_batch();

public:
  ha_lineairdb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_lineairdb() override = default;

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override { return "LineairDB"; }

  /**
    Replace key algorithm with one supported by SE, return the default key
    algorithm for SE if explicit key algorithm was not provided.

    @sa handler::adjust_index_algorithm().
  */
  enum ha_key_alg get_default_index_algorithm() const override
  {
    return HA_KEY_ALG_BTREE;
  }
  bool is_index_algorithm_supported(enum ha_key_alg key_alg) const override
  {
    return key_alg == HA_KEY_ALG_BTREE;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override { return HA_HAS_OWN_BINLOGGING; }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx [[maybe_unused]], uint part [[maybe_unused]],
                    bool all_parts [[maybe_unused]]) const override
  {
    return HA_READ_RANGE;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override
  {
    return HA_MAX_REC_LENGTH;
  }

  uint max_supported_keys() const override { return 4096; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override
  {
    [[maybe_unused]] std::string s;
    return s.max_size();
  }

  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  double scan_time() override
  {
    return (double)(stats.records + stats.deleted) / 20.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  double read_time(uint, uint, ha_rows rows) override
  {
    return (double)rows / 20.0 + 1;
  }

  /*
    Everything below are methods that we implement in ha_lineairdb.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_lineairdb.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def) override; // required

  /** @brief
    We implement this in ha_lineairdb.cc; it's a required method.
  */
  int close(void) override; // required

  int change_active_index(uint keynr);

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_init(uint idx, bool sorted [[maybe_unused]]) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_end() override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data) override;
  // #ifdef INPLACE_UPDATE
  // int update_inplace(const uchar *old_data, uchar *new_data);
  // #endif

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf) override;

  int index_next_same(uchar *buf, const uchar *key, uint key_len) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf) override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) override; // required
  int rnd_end() override;
  int rnd_next(uchar *buf) override;            ///< required
  int rnd_pos(uchar *buf, uchar *pos) override; ///< required
  void position(const uchar *record) override;  ///< required
  int info(uint) override;                      ///< required
  int extra(enum ha_extra_function operation) override;
  int external_lock(THD *thd, int lock_type) override; ///< required
  int start_stmt(THD *thd, thr_lock_type lock_type) override;
  int delete_all_rows(void) override;

  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;
  int delete_table(const char *from, const dd::Table *table_def) override;
  int rename_table(const char *from, const char *to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override; ///< required

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;

  bool inplace_alter_table(TABLE *altered_table,
                           Alter_inplace_info *ha_alter_info,
                           const dd::Table *old_table_def,
                           dd::Table *new_table_def) override;

  THR_LOCK_DATA **store_lock(
      THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type) override; ///< required

  ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                      void *seq_init_param, uint n_ranges,
                                      uint *bufsz, uint *flags, bool *force_default_mrr,
                                      Cost_estimate *cost) override;
  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode,
                            HANDLER_BUFFER *buf) override;

  int multi_range_read_next(char **range_info) override;
  int read_range_first(const key_range *start_key, const key_range *end_key,
                       bool eq_range_arg, bool sorted) override;

private:
  /** The multi range read session object */
  DsMrr_impl m_ds_mrr;
  LineairDBTransaction *&
  get_transaction(THD *thd);

  // Key conversion helpers
  static std::string encode_int_key(const uchar *data, size_t len);
  static std::string encode_datetime_key(const uchar *data, size_t len);
  static std::string encode_string_key(const uchar *data, size_t len);

  static unsigned char key_part_type_tag(LineairDBFieldType type);
  static void append_key_part_encoding(std::string &out, bool is_null,
                                       LineairDBFieldType type,
                                       const std::string &payload);
  static std::string build_prefix_range_end(const std::string &prefix);
  static uint count_used_key_parts(const KEY *key_info, key_part_map keypart_map);
  int fetch_and_set_current_result(uchar *buf, LineairDBTransaction *tx);

  // index_read_map helper functions
  int index_read_primary_key(uchar *buf, const uchar *key, key_part_map keypart_map,
                             enum ha_rkey_function find_flag, KEY *key_info,
                             bool is_prefix_search, LineairDBTransaction *tx);
  int index_read_secondary(uchar *buf, const uchar *key, key_part_map keypart_map,
                           enum ha_rkey_function find_flag, KEY *key_info,
                           bool is_prefix_search, LineairDBTransaction *tx);

  std::string convert_key_to_ldbformat(const uchar *key, key_part_map keypart_map);
  std::string serialize_key_from_field(Field *field);
  std::string build_secondary_key_from_row(const uchar *row_buffer, const KEY &key_info);
  std::string extract_key(const uchar *buf);
  std::string autogenerate_key();
  std::string extract_key_from_mysql(const uchar *row_buffer);

  void set_write_buffer(uchar *buf);
  bool is_primary_key_exists();
  bool is_primary_key_type_int();
  void set_key_and_key_part_info(const TABLE *const table);

  bool store_blob_to_field(Field **field);
  int set_fields_from_lineairdb(uchar *buf, const std::byte *const read_buf,
                                const size_t read_buf_size);
};

#endif /* HA_LINEAIRDB_H */
