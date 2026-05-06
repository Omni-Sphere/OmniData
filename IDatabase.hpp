#pragma once

#include <DataTable.hpp>
#include <SQLParams.hpp>
#include <string>
#include <vector>

namespace omnisphere::services {

class IDatabase {
public:
  virtual ~IDatabase() = default;

  virtual void ConnectionString(const std::string &connectionString) = 0;

  virtual bool Connect() = 0;
  virtual void Disconnect() = 0;

  virtual bool RunStatement(const std::string &query) = 0;
  virtual bool RunPrepared(const std::string &query,
                           const std::vector<omnisphere::types::SQLParam> &params) = 0;

  virtual omnisphere::types::DataTable
  FetchPrepared(const std::string &query,
                const std::vector<omnisphere::types::SQLParam> &params) = 0;
  
  virtual omnisphere::types::DataTable
  FetchPrepared(const std::string &query,
                const std::vector<std::string> &params) = 0;

  virtual omnisphere::types::DataTable 
  FetchPrepared(const std::string &query,
                const std::string &param) = 0;

  virtual omnisphere::types::DataTable FetchResults(const std::string &query) = 0;

  virtual bool BeginTransaction() = 0;
  virtual bool CommitTransaction() = 0;
  virtual bool RollbackTransaction() = 0;
};

} // namespace omnisphere::services
