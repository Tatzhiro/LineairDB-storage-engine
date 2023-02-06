#include "lineairdb_transaction.hh"

LineairDBTransaction::LineairDBTransaction(THD* thd, 
                                            LineairDB::Database* ldb, 
                                            handlerton* lineairdb_hton,
                                            bool isFence) 
    : tx(nullptr), 
      db(ldb), 
      thread(thd), 
      isTransaction(false), 
      hton(lineairdb_hton),
      isFence(isFence) {}

const std::pair<const std::byte *const, const size_t> 
LineairDBTransaction::read(std::string_view key) {
  return tx->Read(key);
}

std::vector<std::string> 
LineairDBTransaction::get_all_keys() {
  std::vector<std::string> keyList;
  tx->Scan("", std::nullopt, [&](auto key, auto) {
    keyList.push_back(std::string(key));
    return false;
  });
  return keyList;
}

void LineairDBTransaction::write(std::string_view key, const std::string value) {
  tx->Write(key, reinterpret_cast<const std::byte*>(value.c_str()),
          value.length());
}

void LineairDBTransaction::delete_value(std::string_view key) {
  tx->Write(key, nullptr, 0);
}


void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  tx = &db->BeginTransaction();

  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  }
  else {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() {
  tx->Abort();
}

void LineairDBTransaction::end_transaction() {
  assert(tx != nullptr);
  db->EndTransaction(*tx, [&](auto) {}); 
  if (isFence) fence();
  delete this;
}

void LineairDBTransaction::fence() const { db->Fence(); }




bool LineairDBTransaction::thd_is_transaction() const {
  return ::thd_test_options(thread, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK);
}

void LineairDBTransaction::register_transaction_to_mysql() {
  const ulonglong threadID = static_cast<ulonglong>(thread->thread_id());
  ::trans_register_ha(thread, isTransaction, hton, &threadID);
}

void LineairDBTransaction::register_single_statement_to_mysql() {
  register_transaction_to_mysql();
}