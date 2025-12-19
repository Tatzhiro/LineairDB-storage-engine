// index_search_plan.hh
// LineairDB Storage Engine: 検索計画を保持する構造体
// @see index_refactor_implementation_plan.md

#ifndef INDEX_SEARCH_PLAN_HH
#define INDEX_SEARCH_PLAN_HH

#include <string>
#include <optional>
#include "my_base.h"

/**
 * @brief 検索操作の種別
 * @see index_refactor_plan.md §3.1
 */
enum class IndexSearchOp
{
    kIndexFirst,       // key == nullptr
    kUniquePoint,      // (PK or UNIQUE) && full key && !nullable-unique
    kSameKeyCursor,    // find_flag == EXACT だが unique point でない
    kRangeMaterialize, // 範囲検索（KEY_OR_NEXT / AFTER_KEY / BEFORE_KEY 等）
    kPrefixLast,       // HA_READ_PREFIX_LAST / LAST_OR_PREV 等
};

/**
 * @brief 検索計画を保持する構造体
 * @see index_refactor_plan.md §3.2
 */
struct IndexSearchPlan
{
    IndexSearchOp op = IndexSearchOp::kRangeMaterialize;

    // 基本情報
    bool is_primary = false;
    uint used_key_parts = 0;
    bool all_parts_specified = false;
    bool is_unique_index = false;    // HA_NOSAME
    bool has_nullable_parts = false; // HA_NULL_PART_KEY
    enum ha_rkey_function find_flag = HA_READ_KEY_EXACT;

    // 境界情報（シリアライズ済み）
    std::string start_key_serialized;
    std::string end_key_serialized;
    std::string exclusive_end_key_serialized; // HA_READ_BEFORE_KEY用

    // same グループ境界（index_next_same用）
    std::string same_group_prefix_serialized;
    std::string same_group_end_serialized;

    // 実行状態
    bool executed = false;

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
        executed = false;
    }
};

#endif // INDEX_SEARCH_PLAN_HH


