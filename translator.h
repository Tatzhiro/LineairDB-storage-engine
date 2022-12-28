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
class Mysql_lineairdb_translator
{
public:
  size_t get_next_field_offset();

  std::string convert_numeric_to_bytes (const size_t num);
  template <typename BYTE_TYPE>
  size_t convert_bytes_to_numeric(const BYTE_TYPE* bytes, const size_t length);

  template <typename BYTE_TYPE>
  std::string&& translate_mysql_field_to_db_field(const BYTE_TYPE* srcMysql, const size_t length);

  void store_null_flags_to_mysql_field(const std::byte* const srcLineairdb, uchar *dstMysql);
  /**
   * @details
   * This method decodes a LineairDB field header info and extracts the field value.
   * This method stores the offset of next field after each execution.
   * Please get_next_field_offset() and pass it to @param offset each time you call this method.
  */
  std::string&& translate_db_field_to_mysql_field(const std::byte* const srcLineairdb, size_t offset);

private:
  size_t valueLength_;
  std::string translatedOutput_;
  size_t nextFieldOffset_;

  size_t get_value_offset(const std::byte* const field, size_t offset);
  void encode_db_header(const size_t input);
};