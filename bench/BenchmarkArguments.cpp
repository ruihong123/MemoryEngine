//
// Created by wang4996 on 7/8/24.
//
#include "BenchmarkArguments.h"
namespace DSMEngine {
    int app_type = -1;
    double scale_factors[2] = { -1, -1 };
    int factor_count = 0;
    int dist_ratio = 1;
    int num_txn = -1;
    int num_core = -1;  // number of cores utilized in a single numa node.

    size_t cache_size = 8 * 1024LLU * 1024LLU* 1024LLU;
    std::string my_host_name;
    unsigned int conn_port = -1;
    std::string config_filename = "../connection.conf";
// To modify tpcc workload
    size_t gReadRatio = 0;
    size_t gTimeLocality = 0;
    bool gForceRandomAccess = false; // fixed
    bool gStandard = true;  // true if follow standard specification

    int FREQUENCY_DELIVERY = 20;  //0 0
    int FREQUENCY_PAYMENT = 20; // 43
    int FREQUENCY_NEW_ORDER = 20; // 45
    int FREQUENCY_ORDER_STATUS = 20;  //0
    int FREQUENCY_STOCK_LEVEL = 20;  //0

}
