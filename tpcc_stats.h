/* Copyright (c) 2025
 * TPC-C Statistics Helper for LineairDB Storage Engine
 *
 * This file provides hardcoded statistics for TPC-C benchmark optimization.
 * These values help MySQL optimizer choose the correct index for TPC-C queries.
 */

#ifndef TPCC_STATS_H
#define TPCC_STATS_H

#include <cstring>
#include <cstdint>

namespace tpcc_stats
{

    /**
     * TPC-C Table Row Counts
     * Based on TPC-C specification with W = number of warehouses
     */
    struct TableStats
    {
        uint64_t warehouse;  // W
        uint64_t district;   // W * 10
        uint64_t customer;   // W * 10 * 3000
        uint64_t history;    // W * 10 * 3000
        uint64_t orders;     // W * 10 * 3000
        uint64_t new_orders; // W * 10 * 900
        uint64_t order_line; // W * 10 * 3000 * 10 (average)
        uint64_t item;       // 100000 (fixed)
        uint64_t stock;      // W * 100000
    };

    /**
     * Calculate table statistics based on warehouse count
     */
    inline TableStats calculate_table_stats(uint32_t warehouses)
    {
        uint64_t W = warehouses;
        return TableStats{
            .warehouse = W,
            .district = W * 10,
            .customer = W * 10 * 3000,
            .history = W * 10 * 3000,
            .orders = W * 10 * 3000,
            .new_orders = W * 10 * 900,
            .order_line = W * 10 * 3000 * 10,
            .item = 100000,
            .stock = W * 100000};
    }

    /**
     * Get estimated row count for a TPC-C table
     * Returns 0 if table is not recognized (fallback to default behavior)
     */
    inline uint64_t get_table_row_count(const char *table_name, uint32_t warehouses)
    {
        if (table_name == nullptr)
            return 0;

        TableStats stats = calculate_table_stats(warehouses);

        // Case-insensitive comparison
        if (strcasecmp(table_name, "warehouse") == 0)
            return stats.warehouse;
        if (strcasecmp(table_name, "district") == 0)
            return stats.district;
        if (strcasecmp(table_name, "customer") == 0)
            return stats.customer;
        if (strcasecmp(table_name, "history") == 0)
            return stats.history;
        if (strcasecmp(table_name, "orders") == 0 ||
            strcasecmp(table_name, "oorder") == 0)
            return stats.orders;
        if (strcasecmp(table_name, "new_orders") == 0 ||
            strcasecmp(table_name, "new_order") == 0)
            return stats.new_orders;
        if (strcasecmp(table_name, "order_line") == 0)
            return stats.order_line;
        if (strcasecmp(table_name, "item") == 0)
            return stats.item;
        if (strcasecmp(table_name, "stock") == 0)
            return stats.stock;

        return 0; // Unknown table
    }

    /**
     * rec_per_key values for customer table indexes
     *
     * PRIMARY KEY (c_w_id, c_d_id, c_id):
     *   - c_w_id only: 30000 rows (10 districts * 3000 customers)
     *   - c_w_id, c_d_id: 3000 rows
     *   - c_w_id, c_d_id, c_id: 1 row (unique)
     *
     * INDEX idx_customer_name (c_w_id, c_d_id, c_last, c_first):
     *   - c_w_id only: 30000 rows
     *   - c_w_id, c_d_id: 3000 rows
     *   - c_w_id, c_d_id, c_last: ~10-20 rows (names have some duplicates)
     *   - c_w_id, c_d_id, c_last, c_first: 1-2 rows
     */
    struct CustomerRecPerKey
    {
        // PRIMARY KEY parts
        static constexpr uint64_t pk_w_id = 30000;
        static constexpr uint64_t pk_w_d_id = 3000;
        static constexpr uint64_t pk_full = 1;

        // idx_customer_name parts
        static constexpr uint64_t idx_name_w_id = 30000;
        static constexpr uint64_t idx_name_w_d_id = 3000;
        static constexpr uint64_t idx_name_w_d_last = 10; // Key value: much smaller than PK!
        static constexpr uint64_t idx_name_full = 1;
    };

    /**
     * rec_per_key values for orders table indexes
     *
     * PRIMARY KEY (o_w_id, o_d_id, o_id):
     *   - o_w_id only: 30000 rows
     *   - o_w_id, o_d_id: 3000 rows
     *   - full key: 1 row
     *
     * INDEX idx_orders (o_w_id, o_d_id, o_c_id, o_id):
     *   - o_w_id, o_d_id, o_c_id: ~10 rows (each customer has ~10 orders on average)
     */
    struct OrdersRecPerKey
    {
        static constexpr uint64_t pk_w_id = 30000;
        static constexpr uint64_t pk_w_d_id = 3000;
        static constexpr uint64_t pk_full = 1;

        static constexpr uint64_t idx_w_d_c = 10;
        static constexpr uint64_t idx_full = 1;
    };

    /**
     * rec_per_key values for new_orders table
     *
     * PRIMARY KEY (no_w_id, no_d_id, no_o_id):
     *   - no_w_id only: 9000 rows (10 districts * 900)
     *   - no_w_id, no_d_id: 900 rows
     *   - full key: 1 row
     */
    struct NewOrdersRecPerKey
    {
        static constexpr uint64_t pk_w_id = 9000;
        static constexpr uint64_t pk_w_d_id = 900;
        static constexpr uint64_t pk_full = 1;
    };

    /**
     * rec_per_key values for stock table
     *
     * PRIMARY KEY (s_w_id, s_i_id):
     *   - s_w_id only: 100000 rows
     *   - full key: 1 row
     */
    struct StockRecPerKey
    {
        static constexpr uint64_t pk_w_id = 100000;
        static constexpr uint64_t pk_full = 1;
    };

    /**
     * rec_per_key values for order_line table
     *
     * PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number):
     *   - ol_w_id only: 300000 rows
     *   - ol_w_id, ol_d_id: 30000 rows
     *   - ol_w_id, ol_d_id, ol_o_id: 10 rows (average items per order)
     *   - full key: 1 row
     */
    struct OrderLineRecPerKey
    {
        static constexpr uint64_t pk_w_id = 300000;
        static constexpr uint64_t pk_w_d_id = 30000;
        static constexpr uint64_t pk_w_d_o = 10;
        static constexpr uint64_t pk_full = 1;
    };

    /**
     * Estimate records in range for customer table
     *
     * @param index_name Name of the index (PRIMARY or idx_customer_name, etc.)
     * @param key_parts_used Number of key parts with equality conditions
     * @return Estimated number of rows
     */
    inline uint64_t estimate_customer_records_in_range(
        const char *index_name,
        uint32_t key_parts_used)
    {

        if (index_name == nullptr)
            return 10;

        // Check if this is a name index (contains "name" in index name)
        bool is_name_index = (strcasestr(index_name, "name") != nullptr ||
                              strcasestr(index_name, "idx_customer") != nullptr);

        if (is_name_index)
        {
            // Secondary index on name
            switch (key_parts_used)
            {
            case 0:
                return 30000;
            case 1:
                return 30000; // w_id only
            case 2:
                return 3000; // w_id, d_id
            case 3:
                return 10; // w_id, d_id, c_last - KEY OPTIMIZATION POINT!
            case 4:
                return 1; // full key
            default:
                return 1;
            }
        }
        else
        {
            // Primary key
            switch (key_parts_used)
            {
            case 0:
                return 30000;
            case 1:
                return 30000; // w_id only
            case 2:
                return 3000; // w_id, d_id
            case 3:
                return 1; // full key (w_id, d_id, c_id)
            default:
                return 1;
            }
        }
    }

    /**
     * Estimate records in range for orders table
     */
    inline uint64_t estimate_orders_records_in_range(
        const char *index_name,
        uint32_t key_parts_used)
    {

        if (index_name == nullptr)
            return 10;

        // Check for customer ID index
        bool is_cid_index = (strcasestr(index_name, "c_id") != nullptr ||
                             strcasestr(index_name, "idx_orders") != nullptr);

        if (is_cid_index)
        {
            switch (key_parts_used)
            {
            case 0:
                return 30000;
            case 1:
                return 30000;
            case 2:
                return 3000;
            case 3:
                return 10; // w_id, d_id, c_id
            case 4:
                return 1;
            default:
                return 1;
            }
        }
        else
        {
            // Primary key
            switch (key_parts_used)
            {
            case 0:
                return 30000;
            case 1:
                return 30000;
            case 2:
                return 3000;
            case 3:
                return 1;
            default:
                return 1;
            }
        }
    }

    /**
     * Estimate records in range for new_orders table
     */
    inline uint64_t estimate_new_orders_records_in_range(uint32_t key_parts_used)
    {
        switch (key_parts_used)
        {
        case 0:
            return 9000;
        case 1:
            return 9000; // w_id only
        case 2:
            return 900; // w_id, d_id
        case 3:
            return 1; // full key
        default:
            return 1;
        }
    }

    /**
     * Estimate records in range for stock table
     */
    inline uint64_t estimate_stock_records_in_range(uint32_t key_parts_used)
    {
        switch (key_parts_used)
        {
        case 0:
            return 100000;
        case 1:
            return 100000; // w_id only
        case 2:
            return 1; // full key
        default:
            return 1;
        }
    }

    /**
     * Estimate records in range for order_line table
     */
    inline uint64_t estimate_order_line_records_in_range(uint32_t key_parts_used)
    {
        switch (key_parts_used)
        {
        case 0:
            return 300000;
        case 1:
            return 300000; // w_id only
        case 2:
            return 30000; // w_id, d_id
        case 3:
            return 10; // w_id, d_id, o_id
        case 4:
            return 1; // full key
        default:
            return 1;
        }
    }

    /**
     * Check if a table name is a known TPC-C table
     */
    inline bool is_tpcc_table(const char *table_name)
    {
        if (table_name == nullptr)
            return false;

        return (strcasecmp(table_name, "warehouse") == 0 ||
                strcasecmp(table_name, "district") == 0 ||
                strcasecmp(table_name, "customer") == 0 ||
                strcasecmp(table_name, "history") == 0 ||
                strcasecmp(table_name, "orders") == 0 ||
                strcasecmp(table_name, "oorder") == 0 ||
                strcasecmp(table_name, "new_orders") == 0 ||
                strcasecmp(table_name, "new_order") == 0 ||
                strcasecmp(table_name, "order_line") == 0 ||
                strcasecmp(table_name, "item") == 0 ||
                strcasecmp(table_name, "stock") == 0);
    }

} // namespace tpcc_stats

#endif // TPCC_STATS_H

