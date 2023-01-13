#include <lineairdb/lineairdb.h>

class LineairDBTransaction
{
private:
  LineairDB::Transaction* tx;
  LineairDB::Database* db;
  bool isTransaction;
public:
  LineairDB::Transaction* get_transaction() { return tx; }
  void set_transaction(LineairDB::Transaction* ldbTx) { tx = ldbTx; }
  void set_isTransaction(bool isTx) { isTransaction = isTx; }
  bool is_transaction() { return isTransaction; }

  LineairDB::Database* get_db() { return db; }
  void set_db(LineairDB::Database* ldb) { db = ldb; }

  void init_transaction() {
    tx = nullptr;
    isTransaction = false;
  }
  
  LineairDBTransaction() { 
    tx = nullptr; 
    isTransaction = false;
  }
  ~LineairDBTransaction() = default;
};
