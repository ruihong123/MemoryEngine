// NOTICE: this file is adapted from Cavalia
#include "TpccRandomGenerator.h"
#include "TpccParams.h"

namespace DSMEngine {
namespace TpccBenchmark {
const std::string TpccRandomGenerator::syllables_[10] = { "BAR", "OUGHT",
    "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
const int TpccRandomGenerator::cLast_ = 1;
//        TpccRandomGenerator::GenerateInteger(0, 255);
const int TpccRandomGenerator::cId_ = 1;
//        TpccRandomGenerator::GenerateInteger(0, 1023);
const int TpccRandomGenerator::orderlineItemId_ = 1;
//    TpccRandomGenerator::GenerateInteger(0, 8191);
}
}
