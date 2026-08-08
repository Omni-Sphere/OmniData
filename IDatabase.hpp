#pragma once

#include "DataTable.hpp"
#include "SQLParams.hpp"
#include <string>
#include <vector>

namespace omnisphere::data
{
    class IDatabase
    {
        public:
        virtual ~IDatabase() = default;

        virtual void ConnectionString(const std::string &connectionString) = 0;

        virtual bool Connect() = 0;
        virtual void Disconnect() = 0;

        virtual bool RunStatement(const std::string &query, const std::string& context = "") = 0;
        virtual bool RunPrepared(const std::string &query,
                                 const std::vector<omnisphere::types::SQLParam> &params,
                                 const std::string& context = "") = 0;

        virtual omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::vector<omnisphere::types::SQLParam> &params,
                      const std::string& context = "") = 0;

        virtual omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::vector<std::string> &params,
                      const std::string& context = "") = 0;

        virtual omnisphere::types::DataTable
        FetchPrepared(const std::string &query,
                      const std::string &param,
                      const std::string& context = "") = 0;

        virtual omnisphere::types::DataTable FetchResults(const std::string &query, const std::string& context = "") = 0;

        virtual bool BeginTransaction() = 0;

        virtual bool CommitTransaction() = 0;
        virtual bool RollbackTransaction() = 0;
    };

} // namespace omnisphere::data
