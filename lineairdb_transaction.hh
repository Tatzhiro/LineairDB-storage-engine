#ifndef LINEAIRDB_TRANSACTION_HH
#define LINEAIRDB_TRANSACTION_HH

#include <lineairdb/lineairdb.h>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mysql/plugin.h"
#include "sql/handler.h" /* handler */
#include "sql/sql_class.h"

class LineairDB_share;

/**
 * @brief
 * Wrapper of LineairDB::Transaction
 * Takes care of registering a transaction to MySQL core
 *
 * Lifetime of this class equals the lifetime of the transaction.
 * The instance of this class is deleted in end_transaction.
 * Set the pointer to this class to nullptr after end_transaction
 * to indicate that LineairDBTransaction is terminated.
 */
class LineairDBTransaction
{
public:
  std::string get_selected_table_name();
  void choose_table(std::string db_table_name);
  bool table_is_not_chosen();

  const std::pair<const std::byte *const, const size_t> read(std::string key);
  std::vector<std::string> get_all_keys();
  std::vector<std::string> get_matching_keys(std::string key);
  std::vector<std::string> get_matching_keys_in_range(std::string start_key, std::string end_key,
                                                      const std::string &exclusive_end_key = "");
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_in_range(
      std::string start_key, std::string end_key,
      const std::string &exclusive_end_key = "");
  const std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view,
                         const std::pair<const void *, const size_t>)>
          operation);
  bool write(std::string key, const std::string value);
  bool write_secondary_index(std::string index_name, std::string secondary_key, const std::string value);
  std::vector<std::pair<const std::byte *const, const size_t>> read_secondary_index(std::string index_name, std::string secondary_key);
  std::vector<std::string> get_matching_primary_keys_in_range(
      std::string index_name, std::string start_key, std::string end_key,
      const std::string &exclusive_end_key = "");

  // Cursor-based prefix search methods
  std::optional<std::string> fetch_first_key_with_prefix(
      const std::string &prefix, const std::string &prefix_end);
  std::optional<std::string> fetch_next_key_with_prefix(
      const std::string &last_key, const std::string &prefix_end);
  bool update_secondary_index(
      std::string index_name,
      std::string old_secondary_key,
      std::string new_secondary_key,
      const std::byte primary_key_buffer[],
      const size_t primary_key_size);

  bool delete_value(std::string key);
  bool delete_secondary_index(std::string index_name, std::string secondary_key, const std::string value);
  void begin_transaction();
  void set_status_to_abort();
  bool end_transaction();
  void fence() const;

  // Per-table committed row-count delta aggregation.
  // Deltas are accumulated within the transaction and flushed only if commit succeeds.
  void add_rowcount_delta(LineairDB_share *share, int64_t delta);
  int64_t peek_rowcount_delta(const LineairDB_share *share) const;

  inline bool is_not_started() const
  {
    if (tx == nullptr)
      return true;
    return false;
  }
  inline bool is_aborted() const
  {
    assert(tx != nullptr);
    return tx->IsAborted();
  }
  inline bool is_a_single_statement() const { return !isTransaction; }

  LineairDBTransaction(THD *thd, LineairDB::Database *ldb,
                       handlerton *lineairdb_hton, bool isFence);
  ~LineairDBTransaction() = default;

private:
  LineairDB::Transaction *tx;
  LineairDB::Database *db;
  std::string db_table_key;
  THD *thread;
  bool isTransaction;
  handlerton *hton;
  bool isFence;
  std::vector<std::pair<LineairDB_share *, int64_t>> rowcount_deltas_;
  bool key_prefix_is_matching(std::string target_key, std::string key);
  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};

#endif // LINEAIRDB_TRANSACTION_HH
