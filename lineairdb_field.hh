#include <vector>
#include <string>
#include <climits>
#include "my_inttypes.h"

/**
 * @brief This class is responsible for the translation between
 * MySQL Field value and LineairDB Field.
 * @details
 * LineairDB field consists of the following 3 information:
 *  header1     header2
 * [byteSize][valueLength][value]
 * header info
 * - byteSize: number of bytes of `valueLength`
 *             always 1 byte, byteSize = UCHAR_MAX if valueLength = 0
 * - valueLength: length of value
 *                max 4 bytes
 * value: MySQL value shown to users
 *        max 4294967295 = sizeof(LONGBLOB) bytes
 * Each row consists of multiple fields.
 * First field stores null flags.
 */
class LineairDBField
{
public:
  std::string convert_numeric_to_bytes (const size_t num);
  template <typename BYTE_TYPE>
  size_t convert_bytes_to_numeric(const BYTE_TYPE* bytes, const size_t length);

  /**
   * @brief These methods are called for INSERT and UPDATE statements
   */
  std::string get_null_field();
  void set_null_field(const uchar* buf, const size_t null_byte_length);
  std::string get_lineairdb_field();
  template <typename CHAR_TYPE>
  void set_lineairdb_field(const CHAR_TYPE* srcMysql, const size_t length);

  /**
   * @brief These methods are called for SELECT statements.
   */
  void make_mysql_table_row(const std::byte* const ldbRawData, const size_t length);
  std::string get_null_flags();
  std::string& get_column_of_row(size_t i);

  LineairDBField() = default;
  
private:
  static constexpr char noValue = 0xff;
  static constexpr size_t maxValueLength = UINT_MAX;

  char byteSize;
  std::string valueLength;
  std::string value;

  std::string nullFlag;
  std::vector<std::string> row;

  void set_header(const size_t num);
  size_t calculate_minimum_byte_size_required(const size_t num);
};