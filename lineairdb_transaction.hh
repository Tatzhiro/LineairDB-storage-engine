#include <lineairdb/lineairdb.h>

class LineairDBTransaction
{
private:
  LineairDB::Transaction* tx;
  LineairDB::Database* db;
  LineairDB::TxStatus txStatus;
public:
  LineairDB::Transaction* get_transaction() { return tx; }
  void set_transaction(LineairDB::Transaction* ldbTx) { tx = ldbTx; }
  void set_status(LineairDB::TxStatus status) { txStatus = status; }
  LineairDB::TxStatus get_status() { return txStatus; }

  LineairDB::Database* get_db() { return db; }
  void set_db(LineairDB::Database* ldb) { db = ldb; }
  
  LineairDBTransaction() { 
    tx = nullptr; 
    txStatus = LineairDB::TxStatus::Running;
  }
  ~LineairDBTransaction() = default;
};
