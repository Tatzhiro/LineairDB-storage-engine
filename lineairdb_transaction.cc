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

std::string LineairDBTransaction::get_selected_table_name() { return db_table_key; }

void LineairDBTransaction::choose_table(std::string db_table_name) {
  db_table_key = db_table_name;
}

bool LineairDBTransaction::table_is_not_chosen() {
  if (db_table_key.size() == 0) {
    std::cout << "Database and Table is not chosen in LineairDBTransaction" << std::endl;
    return true;
  }
  return false;
}

const std::pair<const std::byte *const, const size_t> 
LineairDBTransaction::read(std::string key) {
  if (table_is_not_chosen()) return const std::pair<const std::byte *const, const size_t>{nullptr, 0};
  return tx->Read(db_table_key + key);
}

bool LineairDBTransaction::key_prefix_is_matching(std::string key_prefix, std::string key) {
  if (key.substr(0, key_prefix.size()) != key_prefix) return false;
  return true;
}

std::vector<std::string> 
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};

  std::vector<std::string> keyList;
  tx->Scan("", std::nullopt, [&](auto key, auto) {
    if (key_prefix_is_matching(db_table_key, std::string(key))) {
      keyList.push_back(std::string(key.substr(db_table_key.size())));
    }
    return false;
  });
  return keyList;
}

std::vector<std::string> 
LineairDBTransaction::get_matching_keys(std::string first_key_part) {
  if (table_is_not_chosen()) return {};

  std::vector<std::string> keyList;
  std::string key_prefix{db_table_key + first_key_part};

  tx->Scan("", std::nullopt, [&](auto key, auto) {
    if (key_prefix_is_matching(key_prefix, std::string(key))) {
      keyList.push_back(std::string(key.substr(db_table_key.size())));
    }
    return false;
  });
  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;
  tx->Write(db_table_key + key, reinterpret_cast<const std::byte*>(value.c_str()),
          value.length());
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  tx->Write(db_table_key + key, nullptr, 0);
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