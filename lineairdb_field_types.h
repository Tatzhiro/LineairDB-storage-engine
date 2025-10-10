#ifndef LINEAIRDB_FIELD_TYPES_INCLUDED
#define LINEAIRDB_FIELD_TYPES_INCLUDED

#include "field_types.h"

/**
 * LineairDB internal field types
 */
enum class LineairDBFieldType
{
    // Numeric types
    LINEAIRDB_INT,

    // String types
    LINEAIRDB_STRING,

    // Date/Time types
    LINEAIRDB_DATETIME,

    // Other/Unsupported types
    LINEAIRDB_OTHER
};

/**
 * Convert MySQL field type to LineairDB field type
 *
 * @param mysql_type MySQL's enum_field_types
 * @return Corresponding LineairDB field type
 */
inline LineairDBFieldType convert_mysql_type_to_lineairdb(enum_field_types mysql_type)
{
    switch (mysql_type)
    {
    // Numeric types
    case MYSQL_TYPE_TINY:       // TINYINT
    case MYSQL_TYPE_SHORT:      // SMALLINT
    case MYSQL_TYPE_LONG:       // INT
    case MYSQL_TYPE_LONGLONG:   // BIGINT
    case MYSQL_TYPE_INT24:      // MEDIUMINT
    case MYSQL_TYPE_FLOAT:      // FLOAT
    case MYSQL_TYPE_DOUBLE:     // DOUBLE
    case MYSQL_TYPE_DECIMAL:    // DECIMAL (old)
    case MYSQL_TYPE_NEWDECIMAL: // DECIMAL (new)
    case MYSQL_TYPE_YEAR:       // YEAR
        return LineairDBFieldType::LINEAIRDB_INT;

    // String types
    case MYSQL_TYPE_VARCHAR:     // VARCHAR
    case MYSQL_TYPE_STRING:      // CHAR
    case MYSQL_TYPE_VAR_STRING:  // VAR_STRING
    case MYSQL_TYPE_BLOB:        // BLOB, TEXT
    case MYSQL_TYPE_TINY_BLOB:   // TINYBLOB, TINYTEXT
    case MYSQL_TYPE_MEDIUM_BLOB: // MEDIUMBLOB, MEDIUMTEXT
    case MYSQL_TYPE_LONG_BLOB:   // LONGBLOB, LONGTEXT
    case MYSQL_TYPE_ENUM:        // ENUM
    case MYSQL_TYPE_SET:         // SET
        return LineairDBFieldType::LINEAIRDB_STRING;

    // Date/Time types
    case MYSQL_TYPE_TIMESTAMP:  // TIMESTAMP
    case MYSQL_TYPE_TIMESTAMP2: // TIMESTAMP (internal)
    case MYSQL_TYPE_DATETIME:   // DATETIME
    case MYSQL_TYPE_DATETIME2:  // DATETIME (internal)
    case MYSQL_TYPE_DATE:       // DATE
    case MYSQL_TYPE_TIME:       // TIME
    case MYSQL_TYPE_TIME2:      // TIME (internal)
    case MYSQL_TYPE_NEWDATE:    // NEWDATE (internal)
        return LineairDBFieldType::LINEAIRDB_DATETIME;

    // Other/Unsupported types
    case MYSQL_TYPE_NULL:        // NULL
    case MYSQL_TYPE_BIT:         // BIT
    case MYSQL_TYPE_JSON:        // JSON
    case MYSQL_TYPE_GEOMETRY:    // GEOMETRY
    case MYSQL_TYPE_TYPED_ARRAY: // TYPED_ARRAY (replication)
    case MYSQL_TYPE_BOOL:        // BOOL (placeholder)
    case MYSQL_TYPE_INVALID:     // INVALID
    default:
        return LineairDBFieldType::LINEAIRDB_OTHER;
    }
}

/**
 * Get string representation of LineairDB field type
 *
 * @param type LineairDB field type
 * @return Type name as string
 */
inline const char *lineairdb_field_type_name(LineairDBFieldType type)
{
    switch (type)
    {
    case LineairDBFieldType::LINEAIRDB_INT:
        return "INT";
    case LineairDBFieldType::LINEAIRDB_STRING:
        return "STRING";
    case LineairDBFieldType::LINEAIRDB_DATETIME:
        return "DATETIME";
    case LineairDBFieldType::LINEAIRDB_OTHER:
        return "OTHER";
    default:
        return "UNKNOWN";
    }
}

#endif /* LINEAIRDB_FIELD_TYPES_INCLUDED */
