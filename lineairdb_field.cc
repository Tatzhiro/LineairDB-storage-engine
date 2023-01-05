#include "lineairdb_field.hh"
#include <cassert>

#define BYTE_MAX (256)


/**
 * LineairDBField method definitions
*/

size_t LineairDBField::calculate_minimum_byte_size_required(size_t num) {
  size_t num_bytes = 0;
  while (num > 0) {
    num /= BYTE_MAX;
    ++num_bytes;
  }
  return num_bytes;
}

std::string LineairDBField::convert_numeric_to_bytes(const size_t num) {
  size_t byteSizeOfNum = calculate_minimum_byte_size_required(num);
  std::string byteSequence(&num, &num + byteSizeOfNum);
  return byteSequence;
}

template <typename BYTE_TYPE>
size_t LineairDBField::convert_bytes_to_numeric(const BYTE_TYPE* bytes, const size_t length) {
  size_t n = 0;
  for (size_t i = 0; i < length; i++) {
    n = n | static_cast<const uchar>(bytes[i]) << CHAR_BIT * i;
  }
  return n;
}

template size_t LineairDBField::convert_bytes_to_numeric<std::byte>(const std::byte*, const size_t);

template size_t LineairDBField::convert_bytes_to_numeric<uchar>(const uchar*, const size_t);

std::string LineairDBField::get_null_field() {
  return byteSize + valueLength + nullFlag;
}

std::string LineairDBField::get_lineairdb_field() {
  return byteSize + valueLength + value;
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
  byteSize = convert_numeric_to_bytes(valueLength.size())[0];
}

void LineairDBField::set_null_field(const uchar* buf, const size_t null_byte_length) {
  set_header(null_byte_length);
  nullFlag.clear();
  for (size_t i = 0; i < null_byte_length; i++) nullFlag.push_back(buf[i]);
}

template <typename CHAR_TYPE>
void LineairDBField::set_lineairdb_field(const CHAR_TYPE* srcMysql, const size_t length) {
  set_header(length);
  value.clear();
  for (size_t i = 0; i < length; i++) value.push_back(srcMysql[i]);
}

template void LineairDBField::set_lineairdb_field<char>(const char*, const size_t);

template void LineairDBField::set_lineairdb_field<uchar>(const uchar*, const size_t);

void LineairDBField::make_mysql_table_row(const std::byte* const ldbRawData, const size_t length) {
  row.clear();
  size_t offset = 0;
  while (offset < length) {
    const auto ldbField = ldbRawData + offset;
    byteSize = convert_bytes_to_numeric(ldbField, sizeof(byteSize));
    if (byteSize == noValue) {
      row.emplace_back("");
      offset += sizeof(byteSize);
      continue;
    }

    const size_t valueLength = convert_bytes_to_numeric(ldbField + sizeof(byteSize), byteSize);
    const std::byte* const valueData = ldbField + byteSize + sizeof(byteSize);

    value.clear();
    for (size_t i = 0; i < valueLength; i++) {
      value.push_back(static_cast<char>(valueData[i]));
    }
    if (offset == 0) nullFlag = value;
    else row.emplace_back(value);
    offset += sizeof(byteSize) + byteSize + valueLength;
  }
}

std::string LineairDBField::get_null_flags() { return nullFlag; }

std::string& LineairDBField::get_column_of_row(size_t i) { return row[i]; }