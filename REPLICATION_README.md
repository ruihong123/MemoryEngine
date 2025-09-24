# Data Replication Feature v2

This document describes the data replication feature added to the MemoryEngine storage system.

## Overview

The replication feature allows data to be replicated across multiple physical memory nodes for improved fault tolerance and availability. The system uses logical region IDs instead of physical node IDs in GlobalAddress, and the RDMA manager maps these logical regions to multiple physical memory nodes with configurable sizes.

## Configuration Format v2

The replication feature uses a new configuration file format (`connection_replication.conf`):

```
# Compute nodes (space-separated IP addresses)
10.145.21.34 10.145.21.35 10.145.21.36

# Memory nodes (space-separated IP addresses)
10.145.21.37 10.145.21.38 10.145.21.39

# Logical memory regions with replication and size
# Format: logical_id [size] primary replicas...
# Size syntax: 123456789, 64GiB, 24GB, 128g, 512MiB (case-insensitive)
# Memory indices: 0,1,2... correspond to memory node order
1             64GiB    0        2
3             24GB     1        0
5             128g     2        1  0
```

### Configuration Fields

- **logical_id**: Logical region identifier (used in GlobalAddress.nodeID)
- **size**: Optional size specification (supports various units)
- **primary**: Memory node index for primary replica
- **replicas**: Memory node indices for replica nodes

### Size Syntax

- **Plain bytes**: `123456789`
- **Decimal units**: `KB`, `MB`, `GB`, `TB` (base 10: 10^3) — case-insensitive
- **Binary units**: `KiB`, `MiB`, `GiB`, `TiB` (base 2: 2^10) — case-insensitive
- **Shorthand**: `k`, `m`, `g`, `t` → decimal units

Examples: `65536`, `64GiB`, `24GB`, `128g`, `512MiB`

## Key Changes

### 1. GlobalAddress Structure

The `GlobalAddress` structure now uses `nodeID` as a logical region ID instead of a physical node ID when replication is enabled.

### 2. LogicalGroup Structure

```cpp
// Physical node info for a logical region
struct PhysicalRegion {
    uint16_t phys_id;     // physical node ID
    uint64_t base_ptr;    // base pointer/offset on this physical node
};

// Logical memory group for replication with size support
struct LogicalGroup {
    std::vector<PhysicalRegion> physical_regions; // primary first
    uint64_t bytes{0};   // region size (0 = unspecified)
};
```

### 3. RDMA Manager Extensions

Added new data structures:
- `logical_groups`: Maps logical region IDs to replication groups with sizes and base offsets

## New Methods

### Logical Memory Group Helpers
- `IsLogicalMemoryId()`: Checks if an ID is a logical memory region
- `ResolvePhysicalMemoryNode()`: Resolves logical ID to primary physical node
- `PhysicalRegions()`: Returns physical regions for a logical region
- `RegionBytes()`: Returns configured size for a logical region
- `GetBasePtr()`: Returns base pointer for a logical region on a specific physical node

### Address Translation
The system automatically translates logical addresses to physical addresses using:
- Logical region ID → Physical node ID mapping
- Base offset computation for each physical node
- Automatic bounds checking for region sizes

## Usage Example

```cpp
// Standard configuration (replication is automatic)
struct config_t config = {
    .dev_name = NULL,
    .server_name = NULL,
    .tcp_port = 19843,
    .ib_port = 1,
    .gid_idx = 1,
    .init_local_buffer_size = 0,
    .node_id = 1,
    .replication_num = 3
};

// Set replication config file
strcpy(config_file_name, "connection_replication.conf");

// Create RDMA manager
auto rdma_mg = std::make_shared<RDMA_Manager>(config, remote_block_size);

// Allocate in logical region 1 (automatically replicated)
GlobalAddress addr = rdma_mg->Allocate_Remote_RDMA_Slot(Regular_Page, 1);

// Write (automatically replicated to all configured replicas)
rdma_mg->RDMA_Write(addr, &local_mr, size, flags, poll_num, pool_name);

// Read from a replica
rdma_mg->RDMA_Read(addr, &local_mr, size, flags, poll_num, pool_name);
```

## Backward Compatibility

The system maintains backward compatibility:
- If no logical groups are parsed, the system builds identity groups (no replication)
- If a replication line omits size, it falls back to existing default capacity policy
- All existing code continues to work without modification
- Legacy configuration files continue to work unchanged

## Future Enhancements

1. **Read Load Balancing**: Implement intelligent read load balancing across replicas
2. **Consistency Models**: Add support for different consistency models (strong, eventual)
3. **Failure Detection**: Implement automatic failure detection and replica promotion
4. **Dynamic Reconfiguration**: Support for adding/removing replicas at runtime
5. **Performance Optimization**: Optimize replication performance with parallel writes

## Files Modified

- `storage/rdma.h`: Added replication structures and method declarations
- `storage/rdma.cc`: Implemented replication methods
- `include/Common.h`: Updated GlobalAddress documentation
- `server.cc`: Updated configuration initialization
- `connection_replication.conf`: Example replication configuration
- `example_replication.cpp`: Usage example
