#include <lineairdb/lineairdb.h>

#include "mysql/plugin.h"
#include "sql/handler.h" /* handler */
#include "sql/sql_class.h"

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
  bool write(std::string key, const std::string value);
  bool write_secondary_index(std::string index_name, std::string secondary_key, const std::string value);
  std::vector<std::pair<const std::byte *const, const size_t>> read_secondary_index(std::string index_name, std::string secondary_key);
  std::vector<std::string> get_matching_primary_keys_in_range(
      std::string index_name, std::string start_key, std::string end_key);

  bool delete_value(std::string key);

  void begin_transaction();
  void set_status_to_abort();
  void end_transaction();
  void fence() const;

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

  bool key_prefix_is_matching(std::string target_key, std::string key);
  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};
