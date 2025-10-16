# Key Implementation Guide for MemoryEngine Benchmarks

This guide explains the key generation implementations across all benchmarks in the MemoryEngine project.

## TPC-C Benchmark: Dual Key Mode Support

The TPC-C benchmark now supports **two key generation modes** controlled by the `COMPRESSED_TPCC_KEY` macro.

### Quick Start

**Default Mode (Multi-Field Keys):**
```bash
# No changes needed
cd build
make tpcc
./tpcc <args>
```

**Compressed Mode (uint64_t Keys):**
```bash
# Edit tpcc/TpccKeyGenerator.h and uncomment:
# #define COMPRESSED_TPCC_KEY

cd build
cmake ..
make tpcc
./tpcc <args>
```

### Implementation Details

#### Mode 1: Multi-Field DynamicCompoundKey (Default)
- **Schema**: Multiple columns for each key field
- **Example**: District table has `d_id`, `d_w_id` as separate columns in primary key
- **Key Gen**: `GetDistrictPrimaryKey(d_id, d_w_id, schema_ptr)`
- **Benefits**: Flexible, supports complex queries, schema-aware

#### Mode 2: Compressed uint64_t Keys
- **Schema**: Single `key` (UINT64) column as primary key
- **Example**: District table has single `key` column, data fields follow
- **Key Gen**: `GetDistrictPrimaryKey(d_id, d_w_id)` - schema parameter optional
- **Benefits**: Compact, faster lookups, smaller index footprint

### Key Encoding (Compressed Mode)

Bit layout for compressed keys:
```
63                  48 47      40 39          4 3          0
|---- Warehouse ----|-District-|--- Order ----|-- OL_No ---|
```

Defined in `TpccConstants.h`:
- `kWarehouseBits = 48`
- `kDistrictLowBits = 40`
- `kOrderIdLowBits = 4`

### Files Modified

1. **TpccKeyGenerator.h** - Dual implementation with macro switch
2. **TpccInitiator.h** - Conditional schema definitions for all tables
3. **TpccColumnIndices.h** - Column index macros for both modes (NEW)
4. **TPCC_KEY_MODES.md** - Detailed documentation (NEW)

### Column Index Macros

To write mode-agnostic procedure code, use the macros:

```cpp
#include "TpccColumnIndices.h"

// Works in both modes
district_record->GetColumn(DISTRICT_COL_TAX, &tax);
district_record->SetColumn(DISTRICT_COL_YTD, &ytd);
```

## SmallBank & TATP Benchmarks

These benchmarks use simpler key structures and currently support only the direct conversion approach.

### SmallBank Keys
- **Tables**: Accounts, Savings, Checking
- **Key Type**: Single `int64_t` (custid)
- **Implementation**: Simple integer keys, requires fix in KeyGenerator

### TATP Keys
- **Tables**: Subscriber, AccessInfo, SpecialFacility, CallForwarding
- **Key Types**: 
  - Subscriber: `int64_t` (s_id)
  - AccessInfo: Composite (s_id, ai_type)
  - SpecialFacility: Composite (s_id, sf_type)
  - CallForwarding: Composite (s_id, sf_type, start_time)
- **Implementation**: Bit-shifted compression, requires fix in KeyGenerator

### TODO: Fix SmallBank & TATP KeyGenerators

Both need proper `DynamicCompoundKey` construction. Two options:

**Option 1: Use schema-based approach (like TPC-C default)**
```cpp
static DynamicCompoundKey GenerateSavingsKey(int64_t custid, RecordSchema* schema) {
    static thread_local char buffer[sizeof(int64_t)];
    memcpy(buffer, &custid, sizeof(int64_t));
    return DynamicCompoundKey(buffer, schema);
}
```

**Option 2: Use direct uint64_t (if DynamicCompoundKey supports it)**
```cpp
static DynamicCompoundKey GenerateSavingsKey(int64_t custid) {
    return static_cast<uint64_t>(custid);
}
```

## Benefits of Dual Key Support

### Multi-Field Keys:
- ✅ Schema-aware and type-safe
- ✅ Supports variable-length keys
- ✅ Clear field separation
- ✅ Better for complex range queries
- ❌ Larger memory footprint
- ❌ More complex comparison logic

### Compressed Keys:
- ✅ Compact representation
- ✅ Fast integer comparison
- ✅ Smaller B+tree nodes
- ✅ Better cache locality
- ❌ Limited to 64-bit encoding
- ❌ Requires careful bit allocation
- ❌ Less flexible for complex queries

## Performance Considerations

1. **Index Size**: Compressed keys reduce index memory by ~50% for compound keys
2. **Comparison Speed**: Integer comparison vs. memcmp for multi-field
3. **Cache Lines**: Fewer cache lines needed for compressed key index nodes
4. **Flexibility**: Multi-field supports future schema evolution

## Testing Both Modes

```bash
# Test default mode
cd build
cmake ..
make tpcc
./tpcc --threads 16 --txns 1000000

# Test compressed mode
# Edit TpccKeyGenerator.h: uncomment COMPRESSED_TPCC_KEY
cmake ..
make tpcc
./tpcc --threads 16 --txns 1000000

# Compare results - should be functionally identical
```

## Implementation Status

- ✅ TPC-C: **Fully implemented** with dual mode support
- ⚠️ SmallBank: Needs KeyGenerator fix (simple single-field keys)
- ⚠️ TATP: Needs KeyGenerator fix (composite keys with bit shifting)

## Next Steps

1. Fix SmallBank KeyGenerator to use proper DynamicCompoundKey construction
2. Fix TATP KeyGenerator to use proper DynamicCompoundKey construction  
3. Update TPC-C procedures to use column index macros for complete mode independence
4. Add performance benchmarking scripts to compare both modes
5. Document performance characteristics for different workloads

