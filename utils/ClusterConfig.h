#ifndef __DATABASE_UTILS_CLUSTER_CONFIG_H__
#define __DATABASE_UTILS_CLUSTER_CONFIG_H__

#include <string>
#include <fstream>
#include <vector>
#include "BenchmarkArguments.h"

namespace DSMEngine {
struct ServerInfo {
  ServerInfo(const std::string& addr, const int port)
      : addr_(addr),
        port_no_(port) {
  }
  ServerInfo() {
  }

  std::string addr_;
  int port_no_;
};

class ClusterConfig {
 public:
  ClusterConfig(const std::string& my_host_name_1, const int port,
      const std::string& config_filename_1)
      : my_info_(my_host_name_1, port), config_filename_(config_filename_1) {
    this->ReadConfigFile();
  }
  ~ClusterConfig() {
  }

  ServerInfo GetMyHostInfo() const {
    return my_info_;
  }

  ServerInfo GetMasterHostInfo() const {
    return computes_info_.at(0);
  }

  size_t GetPartitionNum() const {
    return computes_info_.size();
  }
    size_t GetMemoryNum() const {
        return memories_info_.size();
    }

  size_t GetMyPartitionId() const {
    for (size_t i = 0; i < computes_info_.size(); ++i) {
      ServerInfo host = computes_info_.at(i);
      if (host.addr_ == my_info_.addr_ && host.port_no_ == my_info_.port_no_) {
        return i;
      }
    }
    return computes_info_.size() + 1;
  }

  bool IsMaster() const {
    ServerInfo my = GetMyHostInfo();
    ServerInfo master = GetMasterHostInfo();
    return my.addr_ == master.addr_ && my.port_no_ == master.port_no_;
  }

private:
  void ReadConfigFile() {
    std::string name;
    size_t pos = 0;
    std::ifstream readfile(config_filename_);
    std::string space_delimiter = " ";
    assert(readfile.is_open() == true);

      std::getline(readfile,name);
      uint8_t id;
      while ((pos = name.find(space_delimiter)) != std::string::npos) {
          computes_info_.push_back(ServerInfo(name.substr(0, pos), conn_port));
          name.erase(0, pos + space_delimiter.length());
      }
      computes_info_.push_back(ServerInfo(name.substr(0, pos), conn_port));
      std::getline(readfile,name);
      while ((pos = name.find(space_delimiter)) != std::string::npos) {
          memories_info_.push_back(ServerInfo(name.substr(0, pos), conn_port));
          name.erase(0, pos + space_delimiter.length());
      }
      memories_info_.push_back(ServerInfo(name.substr(0, pos), conn_port));
    readfile.close();
  }
private:
  std::vector<ServerInfo> computes_info_;
  std::vector<ServerInfo> memories_info_;
  ServerInfo my_info_;
  std::string config_filename_;
};
}

#endif
