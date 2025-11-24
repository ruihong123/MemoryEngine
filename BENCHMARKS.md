# Additional Benchmarks for MemoryEngine

This document describes the SmallBank and TATP benchmarks that have been added to the MemoryEngine project.

## SmallBank Benchmark

SmallBank is a simple banking benchmark designed to test OLTP systems. It consists of three tables and six transaction types.

### Tables

1. **Accounts**: Customer account information
   - `custid` (INT64): Customer ID
   - `name` (CHAR[64]): Customer name

2. **Savings**: Savings account balances
   - `custid` (INT64): Customer ID
   - `bal` (DOUBLE): Balance

3. **Checking**: Checking account balances
   - `custid` (INT64): Customer ID
   - `bal` (DOUBLE): Balance

### Transactions

1. **Amalgamate** (15%): Transfer all funds from two accounts (savings + checking) of customer 0 to the checking account of customer 1
2. **Balance** (15%): Read-only query to get the total balance (savings + checking) of a customer
3. **DepositChecking** (15%): Deposit money into a checking account
4. **SendPayment** (25%): Transfer money from one checking account to another
5. **TransactSavings** (15%): Add or subtract money from a savings account
6. **WriteCheck** (15%): Write a check, with overdraft penalty if insufficient funds

### Configuration

- Default number of accounts: 100,000
- Balance range: 10,000 to 50,000
- Transaction amount range: 1 to 1,000

### Building and Running

```bash
cd build
cmake ..
make smallbank
./smallbank <arguments>
```

## TATP Benchmark

TATP (Telecom Application Transaction Processing) is a telecommunications industry benchmark that simulates a typical mobile network operator database. It consists of four tables and seven transaction types.

### Tables

1. **Subscriber**: Subscriber information
   - `s_id` (INT64): Subscriber ID
   - `sub_nbr` (CHAR[15]): Subscriber number
   - `bit_` (UINT8[10]): Bit fields
   - `hex_` (UINT16[10]): Hex fields
   - `byte2_` (UINT16[10]): Byte2 fields
   - `msc_location` (UINT32): MSC location
   - `vlr_location` (UINT32): VLR location

2. **AccessInfo**: Access information for subscribers
   - `s_id` (INT64): Subscriber ID
   - `ai_type` (UINT8): Access info type (1-4)
   - `data1` (UINT8): Data field 1
   - `data2` (UINT8): Data field 2
   - `data3` (CHAR[3]): Data field 3
   - `data4` (CHAR[5]): Data field 4

3. **SpecialFacility**: Special facility records
   - `s_id` (INT64): Subscriber ID
   - `sf_type` (UINT8): Special facility type (1-4)
   - `is_active` (UINT8): Active flag
   - `error_cntrl` (UINT8): Error control
   - `data_a` (UINT8): Data field A
   - `data_b` (CHAR[5]): Data field B

4. **CallForwarding**: Call forwarding records
   - `s_id` (INT64): Subscriber ID
   - `sf_type` (UINT8): Special facility type
   - `start_time` (UINT8): Start time (0-2)
   - `end_time` (UINT8): End time (1-24)
   - `numberx` (CHAR[15]): Forwarding number

### Transactions

1. **GetSubscriberData** (35%): Retrieve all subscriber data for a given subscriber ID
2. **GetNewDestination** (10%): Retrieve call forwarding information for a subscriber
3. **GetAccessData** (35%): Retrieve access information for a subscriber
4. **UpdateSubscriberData** (2%): Update subscriber bit field and special facility data
5. **UpdateLocation** (14%): Update VLR location for a subscriber
6. **InsertCallForwarding** (2%): Insert a new call forwarding record
7. **DeleteCallForwarding** (2%): Delete a call forwarding record

### Configuration

- Default number of subscribers: 100,000
- Access info records per subscriber: 4
- Special facility records per subscriber: 3
- Call forwarding records per subscriber: 3 types × 3 start times = 9

### Building and Running

```bash
cd build
cmake ..
make tatp
./tatp <arguments>
```

## Implementation Details

Both benchmarks are implemented using the MemoryEngine's SELCC (Software-Extended Lock Coupling) TransactionManager API. They follow the same architectural pattern as the existing TPC-C benchmark:

- **Constants.h**: Benchmark-specific constants and enumerations
- **Records.h**: Record structures matching table schemas
- **Params.h/.cpp**: Scale parameters and configuration
- **RandomGenerator.h**: Random data generation utilities
- **KeyGenerator.h**: Primary key generation utilities
- **TxnParams.h**: Transaction parameter structures
- **Populator.h**: Database population logic
- **Procedure.h**: Transaction logic implementations
- **Executor.h**: Transaction execution framework
- **Source.h**: Workload generation
- **Initiator.h**: Schema registration and initialization
- **BenchmarkMain.cpp**: Main entry point

## Notes

- Both benchmarks support multiple concurrency control protocols (2PL, OCC, TO, MVOCC) through compile-time flags
- Benchmarks can be run in distributed mode across multiple nodes
- Support for 2-phase commit for distributed transactions
- Configurable workload generation and transaction mix

## References

- SmallBank: https://www.vldb.org/pvldb/vol7/p209-difallah.pdf
- TATP: http://tatpbenchmark.sourceforge.net/

