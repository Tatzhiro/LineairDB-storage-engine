#include "lineairdb_fk_state.hh"

#include <functional>
#include <limits>
#include <stdexcept>

namespace lineairdb::fk::state {
namespace {

constexpr std::size_t kLengthBytes = sizeof(std::uint32_t);

void AppendUint32(std::string &out, std::uint32_t value) {
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    out.push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
  }
}

std::uint32_t ReadUint32(std::string_view encoded, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    value |= static_cast<std::uint32_t>(
                 static_cast<unsigned char>(encoded[offset + i]))
             << (i * 8);
  }
  return value;
}

} // namespace

std::size_t BucketForChild(std::string_view child_primary_key) {
  return std::hash<std::string_view>{}(child_primary_key) % kBucketCount;
}

std::string EncodeRefcount(std::uint64_t count) {
  std::string encoded(sizeof(count), '\0');
  for (std::size_t i = 0; i < sizeof(count); ++i) {
    encoded[i] = static_cast<char>((count >> (i * 8)) & 0xFF);
  }
  return encoded;
}

std::uint64_t DecodeRefcount(std::string_view encoded) {
  if (encoded.empty()) {
    return 0;
  }
  if (encoded.size() != sizeof(std::uint64_t)) {
    throw std::runtime_error("invalid FK refcount payload");
  }

  std::uint64_t value = 0;
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    value |= static_cast<std::uint64_t>(
                 static_cast<unsigned char>(encoded[i]))
             << (i * 8);
  }
  return value;
}

std::vector<std::string> DecodeBucket(std::string_view encoded) {
  std::vector<std::string> children;
  std::size_t offset = 0;
  while (offset < encoded.size()) {
    if (encoded.size() - offset < kLengthBytes) {
      throw std::runtime_error("invalid FK bucket payload");
    }
    std::uint32_t entry_length = ReadUint32(encoded, offset);
    offset += kLengthBytes;
    if (encoded.size() - offset < entry_length) {
      throw std::runtime_error("invalid FK bucket entry length");
    }
    children.emplace_back(encoded.substr(offset, entry_length));
    offset += entry_length;
  }
  return children;
}

bool BucketContains(std::string_view encoded, std::string_view child_primary_key) {
  auto children = DecodeBucket(encoded);
  for (const auto &pk : children) {
    if (pk == child_primary_key) {
      return true;
    }
  }
  return false;
}

bool BucketInsert(std::string &encoded, std::string_view child_primary_key) {
  if (BucketContains(encoded, child_primary_key)) {
    return false;
  }
  if (child_primary_key.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    throw std::runtime_error("child primary key is too large");
  }
  AppendUint32(encoded, static_cast<std::uint32_t>(child_primary_key.size()));
  encoded.append(child_primary_key.data(), child_primary_key.size());
  return true;
}

bool BucketRemove(std::string &encoded, std::string_view child_primary_key) {
  std::string rebuilt;
  bool removed = false;
  std::size_t offset = 0;

  while (offset < encoded.size()) {
    if (encoded.size() - offset < kLengthBytes) {
      throw std::runtime_error("invalid FK bucket payload");
    }
    std::size_t entry_offset = offset;
    std::uint32_t entry_length = ReadUint32(encoded, offset);
    offset += kLengthBytes;
    if (encoded.size() - offset < entry_length) {
      throw std::runtime_error("invalid FK bucket entry length");
    }

    std::string_view current(encoded.data() + offset, entry_length);
    if (current == child_primary_key) {
      removed = true;
    } else {
      rebuilt.append(encoded.data() + entry_offset, kLengthBytes + entry_length);
    }
    offset += entry_length;
  }

  if (removed) {
    encoded.swap(rebuilt);
  }
  return removed;
}

} // namespace lineairdb::fk::state
