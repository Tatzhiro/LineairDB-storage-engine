// index_search_plan.hh
// LineairDB Storage Engine: A structure to hold the search plan

#ifndef INDEX_SEARCH_PLAN_HH
#define INDEX_SEARCH_PLAN_HH

#include <string>
#include <optional>
#include "my_base.h"

/**
 * @brief Types of search operations
 */
enum class IndexSearchOp
{
    kIndexFirst,         // key == nullptr
    kUniquePoint,        // (PK or UNIQUE) && full key && !nullable-unique
    kSameKeyMaterialize, // find_flag == EXACT but not unique point
    kPrefixFirst,        // HA_READ_PREFIX: return first match only
    kRangeMaterialize,   // range search (KEY_OR_NEXT / AFTER_KEY / BEFORE_KEY, etc.)
    kPrevKey,            // HA_READ_KEY_OR_PREV / HA_READ_BEFORE_KEY
    kPrefixLast,         // HA_READ_PREFIX_LAST / LAST_OR_PREV, etc.
};

/**
 * @brief Structure to hold search plan
 */
struct IndexSearchPlan
{
    IndexSearchOp op = IndexSearchOp::kRangeMaterialize;

    // basic information
    bool is_primary = false;
    uint used_key_parts = 0;
    bool all_parts_specified = false;
    bool is_unique_index = false;    // HA_NOSAME
    bool has_nullable_parts = false; // HA_NULL_PART_KEY
    enum ha_rkey_function find_flag = HA_READ_KEY_EXACT;

    // boundary information (serialized)
    std::string start_key_serialized;
    std::string end_key_serialized;
    std::string exclusive_end_key_serialized; // for HA_READ_BEFORE_KEY

    // same group boundary (for index_next_same)
    std::string same_group_prefix_serialized;
    std::string same_group_end_serialized;

    void reset()
    {
        op = IndexSearchOp::kRangeMaterialize;
        is_primary = false;
        used_key_parts = 0;
        all_parts_specified = false;
        is_unique_index = false;
        has_nullable_parts = false;
        find_flag = HA_READ_KEY_EXACT;
        start_key_serialized.clear();
        end_key_serialized.clear();
        exclusive_end_key_serialized.clear();
        same_group_prefix_serialized.clear();
        same_group_end_serialized.clear();
    }
};

#endif // INDEX_SEARCH_PLAN_HH
