#include <string>
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
 *                max 8 bytes
 * value: MySQL value shown to users
 *        max 4294967295 = sizeof(LONGBLOB) bytes
 * Each row consists of multiple fields.
 * First field stores null flags.
 */
class Lineairdb_Field
{
public:
  std::string convert_numeric_to_bytes (const size_t num);
  template <typename BYTE_TYPE>
  size_t convert_bytes_to_numeric(const BYTE_TYPE* bytes, const size_t length);

  /**
   * @brief These methods are called for INSERT and UPDATE statements
   */
  std::string get_lineairdb_field();
  template <typename BYTE_TYPE>
  void set_lineairdb_field(const BYTE_TYPE* srcMysql, const size_t length);

  /**
   * @brief These methods are called for SELECT statements.
   */
  void make_mysql_table_row(const std::byte* const ldbRawData, const size_t length);
  std::string get_null_flags();
  std::string& get_column_of_row(size_t i);

  Lineairdb_Field() {}
  
private:
  static constexpr char noValue = 0xff;

  char byteSize;
  std::string valueLength;
  std::string value;

  std::string nullFlag;
  std::vector<std::string> row;

  void set_header(const size_t num);
};