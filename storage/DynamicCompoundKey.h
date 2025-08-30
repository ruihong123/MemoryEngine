// Copyright (c) 2025, SELCC authors All rights reserved.
//  Use of this source code is governed by a GPL-2.0 license that can be found in the LICENSE file.

//
// Created by ruihong on 8/30/25.
//

#ifndef SELCC_DYNAMICCOMPOUNDKEY_H
#define SELCC_DYNAMICCOMPOUNDKEY_H

#include <shared_mutex>
#include "RecordSchema.h"

namespace DSMEngine {
    class DynamicCompoundKey {
    public:
        // This is a dynamic compound key that can be used in the B-tree.
        // This class is a helper class which enables the directly comparison between the dynamic compound keys.
        char *start = nullptr; // the compind key should always smaller than 1 KB.

        RecordSchema *schema_ptr = nullptr;

        DynamicCompoundKey(char *buff, RecordSchema *schema)
                : start(buff), schema_ptr(schema) {}

        DynamicCompoundKey() {};

        //copy constructor
        DynamicCompoundKey(const DynamicCompoundKey &other) {
            schema_ptr = other.schema_ptr;
            start = other.start;  // shallow copy of the pointer
        }

        void deepcopy_from(const DynamicCompoundKey &other) const {
            assert(schema_ptr == other.schema_ptr || !other.schema_ptr);
            size_t key_length = schema_ptr->GetPrimaryKeyLength();
            std::memcpy(start, other.start, key_length);
        }

        // Move constructor
        DynamicCompoundKey(DynamicCompoundKey &&other) noexcept {
            schema_ptr = other.schema_ptr;
            start = other.start;
            other.start = nullptr;
            other.schema_ptr = nullptr;
        }

        DynamicCompoundKey &operator=(const DynamicCompoundKey &other) {
            schema_ptr = other.schema_ptr;
            start = other.start;
            return *this;
        }

        // Equality ==
        bool operator==(const DynamicCompoundKey &other) const {
            return compare(other) == 0;
        }

        // Inequality !=
        bool operator!=(const DynamicCompoundKey &other) const {
            return compare(other) != 0;
        }

        // Less than <
        bool operator<(const DynamicCompoundKey &other) const {
            return compare(other) < 0;
        }

        // Greater than >
        bool operator>(const DynamicCompoundKey &other) const {
            return compare(other) > 0;
        }

        // Less than or equal <=
        bool operator<=(const DynamicCompoundKey &other) const {
            return compare(other) <= 0;
        }

        // Greater than or equal >=
        bool operator>=(const DynamicCompoundKey &other) const {
            return compare(other) >= 0;
        }

        static DynamicCompoundKey &MinValue() {
            static char value_buff[1024] = {0};
            static DynamicCompoundKey min_key(value_buff, nullptr);
            return min_key;
        }

        static DynamicCompoundKey &MaxValue() {
            static char value_buff[1024] = {static_cast<char>(255)};
            static DynamicCompoundKey max_key(value_buff, nullptr);
            return max_key;
        }
        // Zero-argument sentinels (use the bound schema)
        static DynamicCompoundKey& MinValue(RecordSchema* schema);
        static DynamicCompoundKey& MaxValue(RecordSchema* schema);

    private:
        [[nodiscard]] int compare(const DynamicCompoundKey &other) const {
            using namespace DSMEngine;
            assert(schema_ptr != nullptr || other.schema_ptr != nullptr);
            assert(schema_ptr == other.schema_ptr || !schema_ptr || !other.schema_ptr);
            const RecordSchema *schema = schema_ptr ? schema_ptr : other.schema_ptr;
            size_t num_fields = schema->GetPrimaryColumnCount();
            size_t offset = 0;

            for (size_t i = 0; i < num_fields; ++i) {
                size_t col_id = schema->GetPrimaryColumnId(i);
                size_t size = schema->GetPrimaryColumnSize(i);
                const auto &type = schema->GetColumnType(col_id);

                const char *a = start + offset;
                const char *b = other.start + offset;

                switch (type) {
                    case ValueType::INT: {
                        int32_t va = *reinterpret_cast<const int32_t *>(a);
                        int32_t vb = *reinterpret_cast<const int32_t *>(b);
                        if (va != vb) return va < vb ? -1 : 1;
                        break;
                    }
                    case ValueType::INT64: {
                        int64_t va = *reinterpret_cast<const int64_t *>(a);
                        int64_t vb = *reinterpret_cast<const int64_t *>(b);
                        if (va != vb) return va < vb ? -1 : 1;
                        break;
                    }
                    case ValueType::UINT64: {
                        uint64_t va = *reinterpret_cast<const uint64_t *>(a);
                        uint64_t vb = *reinterpret_cast<const uint64_t *>(b);
                        if (va != vb) return va < vb ? -1 : 1;
                        break;
                    }
                    case ValueType::FIXCHAR: {
                        int cmp = std::memcmp(a, b, size);
                        if (cmp != 0) return cmp < 0 ? -1 : 1;
                        break;
                    }
                    default:
                        fprintf(stderr, "[DynamicCompoundKey] Unsupported type in compare!\n");
                        std::abort();
                }

                offset += size;
            }

            return 0; // all parts equal
        }
    };

    // A footprint that can hold ANY number of columns.
    struct Footprint {
        std::vector<uint32_t> types;  // static_cast<uint32_t>(ValueType)
        std::vector<uint32_t> sizes;  // bytes (in primary-key concatenation order)
        bool operator==(const Footprint &o) const noexcept {
            return types == o.types && sizes == o.sizes;
        }
    };

    struct FootprintHash {
        size_t operator()(const Footprint &f) const noexcept {
            // Simple 64-bit FNV-1a–style mixer over both vectors
            uint64_t h = 1469598103934665603ull;
            auto mix32 = [&](uint32_t x) {
                h ^= x;
                h *= 1099511628211ull;
            };
            auto mixSep = [&]() {
                h ^= 0x9e3779b97f4a7c15ull;
                h *= 1099511628211ull;
            };

            mix32(static_cast<uint32_t>(f.types.size()));
            for (uint32_t t: f.types) mix32(t);
            mixSep();
            mix32(static_cast<uint32_t>(f.sizes.size()));
            for (uint32_t s: f.sizes) mix32(s);

            return static_cast<size_t>(h);
        }
    };

    // Cache entry: owns a stable buffer and provides a DynamicCompoundKey view
    struct Entry {
        std::unique_ptr<unsigned char[]> buf;
        DynamicCompoundKey key;

        Entry(size_t len, RecordSchema *schema)
                : buf(new unsigned char[len]{}),
                  key(reinterpret_cast<char *>(buf.get()), schema) {}
    };
    // Global caches keyed by Footprint; shared/exclusive lock
    namespace dck_cache {
        static std::unordered_map<Footprint, std::unique_ptr<Entry>, FootprintHash> g_min;
        static std::unordered_map<Footprint, std::unique_ptr<Entry>, FootprintHash> g_max;
        static std::shared_mutex g_mu;
    }
    // Thread-local L1 caches: map schema* → sentinel pointer (zero-lock steady state)
    struct PtrHash {
        template <class T> size_t operator()(T* p) const noexcept {
            return std::hash<uintptr_t>{}(reinterpret_cast<uintptr_t>(p));
        }
    };
    static thread_local std::unordered_map<RecordSchema*, DynamicCompoundKey*, PtrHash> tl_min;
    static thread_local std::unordered_map<RecordSchema*, DynamicCompoundKey*, PtrHash> tl_max;

// ---------------- Field-wise writers for typed extrema ----------------

    template <typename T>
    static inline void write_as(void* dst, T v) {
        std::memcpy(dst, &v, sizeof(T));
    }

    static inline void write_min_field(ValueType t, void* dst, size_t len) {
        switch (t) {
            case ValueType::INT32:  write_as<int32_t>(dst,  std::numeric_limits<int32_t>::min()); break;
            case ValueType::INT64:  write_as<int64_t>(dst,  std::numeric_limits<int64_t>::min()); break;
            case ValueType::UINT32: write_as<uint32_t>(dst, std::numeric_limits<uint32_t>::min()); break;
            case ValueType::UINT64: write_as<uint64_t>(dst, std::numeric_limits<uint64_t>::min()); break;
            case ValueType::FLOAT:  write_as<float>(dst,   -std::numeric_limits<float>::infinity()); break;
            case ValueType::DOUBLE: write_as<double>(dst,  -std::numeric_limits<double>::infinity()); break;
                // Fixed-size byte sequences: minimal byte pattern
            default: std::memset(dst, 0x00, len); break; // e.g., CHAR/FIXCHAR/BINARY
        }
    }

    static inline void write_max_field(ValueType t, void* dst, size_t len) {
        switch (t) {
            case ValueType::INT32:  write_as<int32_t>(dst,  std::numeric_limits<int32_t>::max()); break;
            case ValueType::INT64:  write_as<int64_t>(dst,  std::numeric_limits<int64_t>::max()); break;
            case ValueType::UINT32: write_as<uint32_t>(dst, std::numeric_limits<uint32_t>::max()); break;
            case ValueType::UINT64: write_as<uint64_t>(dst, std::numeric_limits<uint64_t>::max()); break;
            case ValueType::FLOAT:  write_as<float>(dst,    std::numeric_limits<float>::infinity()); break;
            case ValueType::DOUBLE: write_as<double>(dst,   std::numeric_limits<double>::infinity()); break;
                // Fixed-size byte sequences: maximal byte pattern
            default: std::memset(dst, 0xFF, len); break; // e.g., CHAR/FIXCHAR/BINARY
        }
    }

// ---- Build a footprint of arbitrary length from the schema’s PRIMARY key ----
    static inline Footprint MakePrimaryFootprint(const RecordSchema* s) {
        Footprint fp;
        const size_t n = s->GetPrimaryColumnCount();
        fp.types.reserve(n);
        fp.sizes.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            const size_t col_id = s->GetPrimaryColumnId(i);
            fp.types.push_back(static_cast<uint32_t>(s->GetColumnType(col_id)));
            fp.sizes.push_back(static_cast<uint32_t>(s->GetPrimaryColumnSize(i)));
        }
        return fp;
    }

// ---- Encode full PRIMARY min/max into a contiguous key buffer ----
    static inline void encode_min_primary(const RecordSchema* s, unsigned char* dst) {
        size_t off = 0;
        const size_t n = s->GetPrimaryColumnCount();
        for (size_t i = 0; i < n; ++i) {
            const size_t col_id = s->GetPrimaryColumnId(i);
            const auto  type    = s->GetColumnType(col_id);
            const size_t sz     = s->GetPrimaryColumnSize(i);
            write_min_field(type, dst + off, sz);
            off += sz;
        }
    }

    static inline void encode_max_primary(const RecordSchema* s, unsigned char* dst) {
        size_t off = 0;
        const size_t n = s->GetPrimaryColumnCount();
        for (size_t i = 0; i < n; ++i) {
            const size_t col_id = s->GetPrimaryColumnId(i);
            const auto  type    = s->GetColumnType(col_id);
            const size_t sz     = s->GetPrimaryColumnSize(i);
            write_max_field(type, dst + off, sz);
            off += sz;
        }
    }

// ---- Construct and publish entries (once per footprint) ----
    static inline Entry* build_min_entry(const Footprint& fp, RecordSchema* schema) {
        const size_t key_len = schema->GetPrimaryKeyLength();
        auto ent = std::make_unique<Entry>(key_len, schema);
        encode_min_primary(schema, ent->buf.get());
        auto raw = ent.get();
        dck_cache::g_min.emplace(fp, std::move(ent));
        return raw;
    }

    static inline Entry* build_max_entry(const Footprint& fp, RecordSchema* schema) {
        const size_t key_len = schema->GetPrimaryKeyLength();
        auto ent = std::make_unique<Entry>(key_len, schema);
        encode_max_primary(schema, ent->buf.get());
        auto raw = ent.get();
        dck_cache::g_max.emplace(fp, std::move(ent));
        return raw;
    }


}
#endif //SELCC_DYNAMICCOMPOUNDKEY_H
