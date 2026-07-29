#pragma once

#include <IDatabase.hpp>
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif
#if defined(ANDROID) || defined(__ANDROID__)
using SQLHENV = void*;
using SQLHDBC = void*;
using SQLHSTMT = void*;
using SQLHANDLE = void*;
using SQLSMALLINT = short;
using SQLLEN = int64_t;
using SQLRETURN = short;
using SQLCHAR = char;
#else
#include <sql.h>
#include <sqlext.h>
#endif

namespace omnisphere::services
{
    class SQLServerDatabase : public IDatabase
    {
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

        bool RunStatement(const std::string &query, const std::string& context = "") override;
        bool RunPrepared(const std::string &query,
                         const std::vector<omnisphere::types::SQLParam> &params,
                         const std::string& context = "") override;

        omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::vector<omnisphere::types::SQLParam> &params,
                      const std::string& context = "") override;
        omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::vector<std::string> &params,
                      const std::string& context = "") override;
        omnisphere::types::DataTable FetchPrepared(const std::string &query,
                                                   const std::string &param,
                                                   const std::string& context = "") override;
        omnisphere::types::DataTable FetchResults(const std::string &query, const std::string& context = "") override;

        bool BeginTransaction() override;

        bool CommitTransaction() override;
        bool RollbackTransaction() override;
    };

} // namespace omnisphere::services
