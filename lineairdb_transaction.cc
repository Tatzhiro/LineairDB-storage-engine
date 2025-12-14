#include "lineairdb_transaction.hh"

#include <utility>

LineairDBTransaction::LineairDBTransaction(THD *thd, LineairDB::Database *ldb,
                                           handlerton *lineairdb_hton,
                                           bool isFence)
    : tx(nullptr),
      db(ldb),
      thread(thd),
      isTransaction(false),
      hton(lineairdb_hton),
      isFence(isFence) {}

std::string LineairDBTransaction::get_selected_table_name()
{
  return db_table_key;
}

void LineairDBTransaction::choose_table(std::string db_table_name)
{
  db_table_key = db_table_name;
  tx->SetTable(db_table_key);
}

bool LineairDBTransaction::table_is_not_chosen()
{
  if (db_table_key.size() == 0)
  {
    return true;
  }
  return false;
}

const std::pair<const std::byte *const, const size_t>
LineairDBTransaction::read(std::string key)
{
  if (table_is_not_chosen())
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};

  auto result = tx->Read(key);
  return result;
}

std::vector<std::pair<const std::byte *const, const size_t>>
LineairDBTransaction::read_secondary_index(std::string index_name, std::string secondary_key)
{
  if (table_is_not_chosen())
    return {};

  auto result = tx->ReadSecondaryIndex(index_name, secondary_key);
  return result;
}

bool LineairDBTransaction::update_secondary_index(
    std::string index_name,
    std::string old_secondary_key,
    std::string new_secondary_key,
    const std::byte primary_key_buffer[],
    const size_t primary_key_size)
{
  tx->UpdateSecondaryIndex(index_name, old_secondary_key, new_secondary_key,
                           primary_key_buffer, primary_key_size);
  return true;
}

bool LineairDBTransaction::key_prefix_is_matching(std::string key_prefix,
                                                  std::string key)
{
  if (key.substr(0, key_prefix.size()) != key_prefix)
    return false;
  return true;
}

std::vector<std::string> LineairDBTransaction::get_all_keys()
{
  if (table_is_not_chosen())
    return {};

  std::vector<std::string> keyList;
  tx->Scan("", std::nullopt, [&](auto key, auto)
           {
    std::string key_str(key);
    auto value = tx->Read(key_str);
    if (value.second == 0 || value.first == nullptr)
    {
      // tombstone: skip
      return false;
    }
    keyList.push_back(std::move(key_str));
    return false; });
  return keyList;
}

std::vector<std::string> LineairDBTransaction::get_matching_primary_keys_in_range(
    std::string index_name, std::string start_key, std::string end_key,
    const std::string &exclusive_end_key)
{
  if (table_is_not_chosen())
    return {};

  std::vector<std::string> result;

  tx->ScanSecondaryIndex(
      index_name,
      start_key,
      end_key,
      [&result, &exclusive_end_key](std::string_view secondary_key, const std::vector<std::string> &primary_keys)
      {
        // Skip if secondary_key matches exclusive end key (HA_READ_BEFORE_KEY)
        if (!exclusive_end_key.empty() && secondary_key == exclusive_end_key)
        {
          return false;
        }
        for (const auto &pk : primary_keys)
        {
          result.push_back(pk);
        }
        return false;
      });

  return result;
}

std::vector<std::string> LineairDBTransaction::get_matching_keys(
    std::string first_key_part)
{
  if (table_is_not_chosen())
    return {};

  std::vector<std::string> keyList;
  std::string key_prefix{first_key_part};

  tx->Scan("", std::nullopt, [&](auto key, auto)
           {
    if (key_prefix_is_matching(key_prefix, std::string(key))) {
      keyList.push_back(std::string(key));
    }
    return false; });
  return keyList;
}

std::vector<std::string> LineairDBTransaction::get_matching_keys_in_range(
    std::string start_key, std::string end_key,
    const std::string &exclusive_end_key)
{
  if (table_is_not_chosen())
    return {};

  std::vector<std::string> keyList;

  tx->Scan(start_key, end_key, [&keyList, &exclusive_end_key](auto key, auto)
           {
    // Skip if key matches exclusive end key (HA_READ_BEFORE_KEY)
    if (!exclusive_end_key.empty() && key == exclusive_end_key)
    {
      return false;
    }
    keyList.push_back(std::string(key));
    return false; });

  return keyList;
}

const std::optional<size_t>
LineairDBTransaction::Scan(std::string_view begin,
                           std::optional<std::string_view> end,
                           std::function<bool(std::string_view,
                                              const std::pair<const void *,
                                                              const size_t>)>
                               operation)
{
  if (table_is_not_chosen())
  {
    return std::nullopt;
  }
  return tx->Scan(begin, end, std::move(operation));
}

std::optional<std::string> LineairDBTransaction::fetch_first_key_with_prefix(
    const std::string& prefix, const std::string& prefix_end)
{
  if (table_is_not_chosen())
    return std::nullopt;

  std::optional<std::string> result;
  tx->Scan(prefix, prefix_end, [&result](auto key, auto value) {
    // Skip tombstones
    if (value.first == nullptr || value.second == 0) {
      return false;  // Continue scanning
    }
    result = std::string(key);
    return true;  // Stop after first valid key
  });
  return result;
}

std::optional<std::string> LineairDBTransaction::fetch_next_key_with_prefix(
    const std::string& last_key, const std::string& prefix_end)
{
  if (table_is_not_chosen())
    return std::nullopt;

  std::optional<std::string> result;
  bool skip_first = true;

  tx->Scan(last_key, prefix_end, [&result, &skip_first, &last_key](auto key, auto value) {
    // Skip the last_key itself (we want the next one)
    if (skip_first && key == last_key) {
      skip_first = false;
      return false;  // Continue scanning
    }
    // Skip tombstones
    if (value.first == nullptr || value.second == 0) {
      return false;  // Continue scanning
    }
    result = std::string(key);
    return true;  // Stop after first valid key
  });
  return result;
}

bool LineairDBTransaction::write(std::string key, const std::string value)
{
  if (table_is_not_chosen())
    return false;
  tx->Write(key,
            reinterpret_cast<const std::byte *>(value.c_str()), value.length());
  return true;
}

bool LineairDBTransaction::write_secondary_index(std::string index_name, std::string secondary_key, const std::string value)
{
  if (table_is_not_chosen())
    return false;

  tx->WriteSecondaryIndex(index_name, secondary_key,
                          reinterpret_cast<const std::byte *>(value.c_str()), value.length());
  return true;
}

bool LineairDBTransaction::delete_value(std::string key)
{
  if (table_is_not_chosen())
    return false;
  tx->Delete(key);
  return true;
}

bool LineairDBTransaction::delete_secondary_index(std::string index_name, std::string secondary_key, const std::string value)
{
  if (table_is_not_chosen())
    return false;
  tx->DeleteSecondaryIndex(index_name, secondary_key, reinterpret_cast<const std::byte *>(value.c_str()), value.length());
  return true;
}

void LineairDBTransaction::begin_transaction()
{
  assert(is_not_started());
  tx = &db->BeginTransaction();

  if (thd_is_transaction())
  {
    isTransaction = true;
    register_transaction_to_mysql();
  }
  else
  {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() { tx->Abort(); }

void LineairDBTransaction::end_transaction()
{
  // tx may be nullptr for DDL operations like CREATE INDEX
  if (tx != nullptr)
  {
    bool was_aborted = tx->IsAborted();
    db->EndTransaction(*tx, [&](auto) {});
    // Skip fence() if transaction was aborted to avoid deadlock
    if (isFence && !was_aborted)
    {
      fence();
    }
  }
  delete this;
}

void LineairDBTransaction::fence() const { db->Fence(); }

bool LineairDBTransaction::thd_is_transaction() const
{
  return ::thd_test_options(
      thread, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK);
}

void LineairDBTransaction::register_transaction_to_mysql()
{
  const ulonglong threadID = static_cast<ulonglong>(thread->thread_id());
  ::trans_register_ha(thread, isTransaction, hton, &threadID);
}

void LineairDBTransaction::register_single_statement_to_mysql()
{
  register_transaction_to_mysql();
}