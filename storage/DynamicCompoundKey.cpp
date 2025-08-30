// Copyright (c) 2025, SELCC authors All rights reserved.
//  Use of this source code is governed by a GPL-2.0 license that can be found in the LICENSE file.

//
// Created by ruihong on 8/30/25.
//

#include "DynamicCompoundKey.h"
// ------------------------- Public zero-arg API -------------------------
namespace DSMEngine {
    DynamicCompoundKey &DynamicCompoundKey::MinValue(RecordSchema* schema) {
        assert(schema && "Call BindPrimaryKeySchema(schema) before MinValue().");

        // L1: thread-local hit → zero locks
        if (auto it = tl_min.find(schema); it != tl_min.end())
            return *it->second;

        // L2: global cache keyed by arbitrary-length footprint
        const Footprint fp = MakePrimaryFootprint(schema);
        {
            std::shared_lock sh(dck_cache::g_mu);
            if (auto it = dck_cache::g_min.find(fp); it != dck_cache::g_min.end()) {
                auto *k = &it->second->key;
                tl_min.emplace(schema, k);
                return *k;
            }
        }
        // Miss: build once (double-checked under exclusive lock)
        {
            std::unique_lock ex(dck_cache::g_mu);
            if (auto it = dck_cache::g_min.find(fp); it != dck_cache::g_min.end()) {
                auto *k = &it->second->key;
                tl_min.emplace(schema, k);
                return *k;
            }
            auto *e = build_min_entry(fp, schema);
            auto *k = &e->key;
            tl_min.emplace(schema, k);
            return *k;
        }
    }

    DynamicCompoundKey &DynamicCompoundKey::MaxValue(RecordSchema* schema) {
        assert(schema && "Call BindPrimaryKeySchema(schema) before MaxValue().");

        if (auto it = tl_max.find(schema); it != tl_max.end())
            return *it->second;

        const Footprint fp = MakePrimaryFootprint(schema);
        {
            std::shared_lock sh(dck_cache::g_mu);
            if (auto it = dck_cache::g_max.find(fp); it != dck_cache::g_max.end()) {
                auto *k = &it->second->key;
                tl_max.emplace(schema, k);
                return *k;
            }
        }
        {
            std::unique_lock ex(dck_cache::g_mu);
            if (auto it = dck_cache::g_max.find(fp); it != dck_cache::g_max.end()) {
                auto *k = &it->second->key;
                tl_max.emplace(schema, k);
                return *k;
            }
            auto *e = build_max_entry(fp, schema);
            auto *k = &e->key;
            tl_max.emplace(schema, k);
            return *k;
        }
    }
}