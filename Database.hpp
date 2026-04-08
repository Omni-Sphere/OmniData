#pragma once

#include "IDatabase.hpp"
#include <memory>
#include <string>

namespace omnisphere::services {

enum class DatabaseEngine {
  SQLServer,
  MySQL
};

class Database : public IDatabase {
private:
  std::unique_ptr<IDatabase> _impl;

public:
  Database(DatabaseEngine engine = DatabaseEngine::SQLServer);
  ~Database() override = default;

  // Delegated IDatabase methods
  void ConnectionString(const std::string &connectionString) override {
    _impl->ConnectionString(connectionString);
  }

  bool Connect() override { return _impl->Connect(); }
  void Disconnect() override { _impl->Disconnect(); }

  bool RunStatement(const std::string &query) override { return _impl->RunStatement(query); }
  bool RunPrepared(const std::string &query,
                   const std::vector<omnisphere::types::SQLParam> &params) override {
    return _impl->RunPrepared(query, params);
  }

  omnisphere::types::DataTable
  FetchPrepared(const std::string &query,
                const std::vector<omnisphere::types::SQLParam> &params) override {
    return _impl->FetchPrepared(query, params);
  }
  
  omnisphere::types::DataTable
  FetchPrepared(const std::string &query,
                const std::vector<std::string> &params) override {
    return _impl->FetchPrepared(query, params);
  }

  omnisphere::types::DataTable 
  FetchPrepared(const std::string &query,
                const std::string &param) override {
    return _impl->FetchPrepared(query, param);
  }

  omnisphere::types::DataTable FetchResults(const std::string &query) override {
    return _impl->FetchResults(query);
  }

  bool BeginTransaction() override { return _impl->BeginTransaction(); }
  bool CommitTransaction() override { return _impl->CommitTransaction(); }
  bool RollbackTransaction() override { return _impl->RollbackTransaction(); }
};

} // namespace omnisphere::services