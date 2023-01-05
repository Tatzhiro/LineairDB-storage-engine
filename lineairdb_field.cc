#include "lineairdb_field.hh"
#include <cassert>

#define BYTE_MAX (256)


/**
 * LineairDBField method definitions
*/

inline size_t LineairDBField::calculate_minimum_byte_size_required(const size_t num) const {
  size_t num_bytes = 0;
  for (size_t n = num; n > 0; n /= BYTE_MAX) num_bytes++;
  return num_bytes;
}

char LineairDBField::convert_numeric_to_a_byte(const size_t num) const {
  return convert_numeric_to_bytes(num)[0];
}

std::string LineairDBField::convert_numeric_to_bytes(const size_t num) const {
  size_t byteSizeOfNum = calculate_minimum_byte_size_required(num);
  std::string byteSequence(&num, &num + byteSizeOfNum);
  return byteSequence;
}

size_t LineairDBField::convert_bytes_to_numeric(
    std::variant<const std::byte*, const uchar*> bytes, 
    const size_t length) const {
  size_t n = 0;
  for (size_t i = 0; i < length; i++) {
    std::visit([&](auto&& oneByte) {
      n = n | static_cast<uchar>(oneByte[i]) << CHAR_BIT * i;
    }, bytes);
  }
  return n;
}


std::string LineairDBField::get_null_field() const {
  return get_lineairdb_field();
}

std::string LineairDBField::get_lineairdb_field() const {
  return std::move(byteSize + valueLength + value);
}

void LineairDBField::set_header(const size_t num) {
  if (num == 0) {
    byteSize = noValue;
    valueLength.clear();
    value.clear();
    return;
  }
  assert(num <= maxValueLength);
  valueLength = convert_numeric_to_bytes(num);
  byteSize = convert_numeric_to_a_byte(valueLength.size());
}

void LineairDBField::set_null_field(const uchar* const buf, const size_t null_byte_length) {
  set_lineairdb_field(buf, null_byte_length);
}

void LineairDBField::set_lineairdb_field(
    std::variant<const uchar*, const char*> const srcMysql, 
    const size_t length) {
  set_header(length);
  std::visit([&](auto&& src) {
    value.assign(reinterpret_cast<const char*>(src), length);
  }, srcMysql);
}

void LineairDBField::make_mysql_table_row(const std::byte* const ldbRawData, const size_t length) {
  row.clear();
  for (size_t offset = 0; offset < length;) {
    const auto ldbField = ldbRawData + offset;
    byteSize = convert_bytes_to_numeric(ldbField, sizeof(byteSize));
    if (byteSize == noValue) {
      row.emplace_back("");
      offset += sizeof(byteSize);
      continue;
    }

    const size_t valueLength = convert_bytes_to_numeric(ldbField + sizeof(byteSize), byteSize);
    assert(valueLength <= maxValueLength);
    const auto valueData = ldbField + byteSize + sizeof(byteSize);

    value.assign(reinterpret_cast<const char*>(valueData), valueLength);
    if (offset == 0) nullFlag = value;
    else row.emplace_back(value);
    offset += sizeof(byteSize) + byteSize + valueLength;
  }
}

const std::string& LineairDBField::get_null_flags() const { return nullFlag; }

const std::string& LineairDBField::get_column_of_row(const size_t i) const { return row[i]; }
