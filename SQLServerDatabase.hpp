#pragma once

#include "IDatabase.hpp"
#include <sql.h>
#include <sqlext.h>

namespace omnisphere::services {

class SQLServerDatabase : public IDatabase {
private:
  SQLHENV henv;
  SQLHDBC hdbc;
  SQLHSTMT hstmt;

  std::string _ConnectionString;

  void PrepareStatement(const std::string &);
  std::string ExtractError(const char *, SQLHANDLE, SQLSMALLINT);

  std::vector<double> doubleStorage;
  std::vector<std::string> stringStorage;
  std::vector<std::vector<uint8_t>> binaryStorage;
  std::vector<int> intStorage;
  std::vector<SQLLEN> indStorage;

public:
  SQLServerDatabase();
  ~SQLServerDatabase() override;

  void ConnectionString(const std::string &connectionString) override;

  bool Connect() override;
  void Disconnect() override;

  bool RunStatement(const std::string &query) override;
  bool RunPrepared(const std::string &query,
                   const std::vector<omnisphere::types::SQLParam> &params) override;

  omnisphere::types::DataTable
  FetchPrepared(const std::string &query,
                const std::vector<omnisphere::types::SQLParam> &params) override;
  omnisphere::types::DataTable
  FetchPrepared(const std::string &query,
                const std::vector<std::string> &params) override;
  omnisphere::types::DataTable FetchPrepared(const std::string &query,
                                             const std::string &param) override;
  omnisphere::types::DataTable FetchResults(const std::string &query) override;

  bool BeginTransaction() override;
  bool CommitTransaction() override;
  bool RollbackTransaction() override;
};

} // namespace omnisphere::services
