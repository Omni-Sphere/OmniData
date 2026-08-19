#pragma once

#include "IDatabase.hpp"
#include <mysql/mysql.h>
#include <string>
#include <vector>

namespace omnisphere::data
{
    class MySQLDatabase : public IDatabase
    {
    private:
        MYSQL* _conn = nullptr;
        std::string _connectionString;

        // Parsed from connection string
        std::string  _host;
        std::string  _user;
        std::string  _password;
        std::string  _database;
        unsigned int _port = 3306;

        std::string ExtractError() const;
        std::string ExtractStmtError(MYSQL_STMT* stmt) const;
        void ParseConnectionString(const std::string& cs);

        // Build MYSQL_BIND[] for input parameters
        struct ParamStorage
        {
            int8_t            i8  = 0;
            int32_t           i32 = 0;
            int64_t           i64 = 0;
            double            f64 = 0.0;
            std::string       str;
            std::vector<uint8_t> blob;
            my_bool           isNull = 0;
            unsigned long     length = 0;
        };

        static void BuildParamBinds(const std::vector<omnisphere::types::SQLParam>& params,
                                    std::vector<ParamStorage>& storage,
                                    std::vector<MYSQL_BIND>& binds);

        omnisphere::types::DataTable FetchFromStatement(MYSQL_STMT* stmt);

    public:
        MySQLDatabase();
        ~MySQLDatabase() override;

        void ConnectionString(const std::string& connectionString) override;

        bool Connect()    override;
        void Disconnect() override;

        bool RunStatement(const std::string& query, const std::string& context = "") override;
        bool RunPrepared(const std::string& query,
                         const std::vector<omnisphere::types::SQLParam>& params,
                         const std::string& context = "") override;

        omnisphere::types::DataTable
        FetchPrepared(const std::string& query,
                      const std::vector<omnisphere::types::SQLParam>& params,
                      const std::string& context = "") override;
        omnisphere::types::DataTable
        FetchPrepared(const std::string& query,
                      const std::vector<std::string>& params,
                      const std::string& context = "") override;
        omnisphere::types::DataTable
        FetchPrepared(const std::string& query,
                      const std::string& param,
                      const std::string& context = "") override;
        omnisphere::types::DataTable FetchResults(const std::string& query,
                                                  const std::string& context = "") override;

        bool BeginTransaction()  override;
        bool CommitTransaction() override;
        bool RollbackTransaction() override;
    };

} // namespace omnisphere::data
