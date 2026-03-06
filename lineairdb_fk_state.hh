#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace lineairdb::fk::state {

constexpr std::size_t kBucketCount = 64;

std::size_t BucketForChild(std::string_view child_primary_key);

std::string EncodeRefcount(std::uint64_t count);
std::uint64_t DecodeRefcount(std::string_view encoded);

std::vector<std::string> DecodeBucket(std::string_view encoded);
bool BucketContains(std::string_view encoded, std::string_view child_primary_key);
bool BucketInsert(std::string &encoded, std::string_view child_primary_key);
bool BucketRemove(std::string &encoded, std::string_view child_primary_key);

} // namespace lineairdb::fk::state
