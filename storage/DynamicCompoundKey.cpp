// Copyright (c) 2025, SELCC authors All rights reserved.
//  Use of this source code is governed by a GPL-2.0 license that can be found in the LICENSE file.

//
// Created by ruihong on 8/30/25.
//

#include "DynamicCompoundKey.h"
#include <mutex>
// ------------------------- Public zero-arg API -------------------------
namespace DSMEngine {
    DynamicCompoundKey DynamicCompoundKey::MinValue(RecordSchema* schema) {
        assert(schema);
        const Footprint fp = MakePrimaryFootprint(schema);

        // L1: thread-local
        if (auto it = tl_min.find(fp); it != tl_min.end()) {
            return {reinterpret_cast<char*>(it->second->buf.get()), schema};
        }

        // L2: global (double-checked)
        Entry* e = nullptr;
        {
            std::shared_lock sh(dck_cache::g_mu);
            if (auto it = dck_cache::g_min.find(fp); it != dck_cache::g_min.end())
                e = it->second.get();
        }
        if (!e) {
            std::unique_lock ex(dck_cache::g_mu);
            if (auto it = dck_cache::g_min.find(fp); it != dck_cache::g_min.end())
                e = it->second.get();
            else
                e = build_min_entry(fp, schema);
        }
        tl_min.emplace(fp, e);
        return {reinterpret_cast<char*>(e->buf.get()), schema};
    }

    DynamicCompoundKey DynamicCompoundKey::MaxValue(RecordSchema* schema) {
        assert(schema);
        const Footprint fp = MakePrimaryFootprint(schema);

        if (auto it = tl_max.find(fp); it != tl_max.end()) {
            return {reinterpret_cast<char*>(it->second->buf.get()), schema};
        }

        Entry* e = nullptr;
        {
            std::shared_lock sh(dck_cache::g_mu);
            if (auto it = dck_cache::g_max.find(fp); it != dck_cache::g_max.end())
                e = it->second.get();
        }
        if (!e) {
            std::unique_lock ex(dck_cache::g_mu);
            if (auto it = dck_cache::g_max.find(fp); it != dck_cache::g_max.end())
                e = it->second.get();
            else
                e = build_max_entry(fp, schema);
        }
        tl_max.emplace(fp, e);
        return {reinterpret_cast<char*>(e->buf.get()), schema};
    }
}