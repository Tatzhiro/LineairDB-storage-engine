#include <lineairdb/lineairdb.h>
#include "sql/handler.h" /* handler */
#include "mysql/plugin.h"
#include "sql/sql_class.h"

/**
 * @brief 
 * Wrapper of LineairDB::Transaction
 * Takes care of registering a transaction to MySQL core
 * Lifetime of this class equals the lifetime of the transaction
 * The instance of this class must be deleted after `end_transaction`
 */
class LineairDBTransaction
{
public:
  const std::pair<const std::byte *const, const size_t> read(std::string_view key);
  std::vector<std::string> get_all_keys();
  void write(std::string_view key, const std::string value);
  void delete_value(std::string_view key);


  void begin_transaction();
  void set_status_to_abort();
  void end_transaction();
  void fence() const;
  

  inline bool is_not_started() const {
    if (tx == nullptr) return true;
    return false;
  }
  inline bool is_aborted() const {
    assert(tx != nullptr);
    return tx->IsAborted();
  }
  inline bool is_a_single_statement() const { return !isTransaction; }


  LineairDBTransaction(THD* thd, LineairDB::Database* ldb, handlerton* lineairdb_hton);
  ~LineairDBTransaction() = default;

private:
  LineairDB::Transaction* tx;
  LineairDB::Database* db;
  THD* thread;
  bool isTransaction;
  handlerton* hton;

  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};
