#pragma once

#include "IDatabase.hpp"
#include <memory>
#include <string>

namespace omnisphere::data
{
    enum class DatabaseEngine
    {
        SQLServer,
        MySQL,
        PostgreSQL
    };

    struct ConnectionConfig
    {
        std::string server;
        std::string database;
        std::string user;
        std::string password;
        DatabaseEngine dbEngine;
        bool trustCertificate = true;
        bool trustedConnection = false;
    };

    class Database : public IDatabase
    {
        private:
        std::unique_ptr<IDatabase> _impl;

        public:
        Database(DatabaseEngine engine = DatabaseEngine::SQLServer);
        Database(const ConnectionConfig &connectionConfig);

        ~Database() override;

        static std::string BuildConnectionString(const ConnectionConfig& cfg);

        static std::string BuildConnectionString(
            DatabaseEngine engine,
            const std::string& server,
            const std::string& database,
            const std::string& user,
            const std::string& password,
            bool trustCert = true,
            bool trustedConn = false
        );

        // Delegated IDatabase methods
        void ConnectionString(const std::string &connectionString) override
        {
            _impl->ConnectionString(connectionString);
        }

        bool Connect() override
        { return _impl->Connect(); }
        void Disconnect() override
        { _impl->Disconnect(); }

        bool RunStatement(const std::string &query, const std::string& context = "") override
        {
            return _impl->RunStatement(query, context);
        }
        bool RunPrepared(const std::string &query,
                         const std::vector<omnisphere::types::SQLParam> &params,
                         const std::string& context = "") override
        {
            return _impl->RunPrepared(query, params, context);
        }

        omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::vector<omnisphere::types::SQLParam> &params,
                      const std::string& context = "") override
        {
            return _impl->FetchPrepared(query, params, context);
        }

        omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::vector<std::string> &params,
                      const std::string& context = "") override
        {
            return _impl->FetchPrepared(query, params, context);
        }

        omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::string &param,
                      const std::string& context = "") override
        {
            return _impl->FetchPrepared(query, param, context);
        }

        omnisphere::types::DataTable FetchResults(const std::string &query, const std::string& context = "") override
        {
            return _impl->FetchResults(query, context);
        }

        bool BeginTransaction() override
        { return _impl->BeginTransaction(); }

        bool CommitTransaction() override

        { return _impl->CommitTransaction(); }
        bool RollbackTransaction() override
        { return _impl->RollbackTransaction(); }
    };

} // namespace omnisphere::data