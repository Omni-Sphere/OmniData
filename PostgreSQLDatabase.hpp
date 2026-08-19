#pragma once

#include "IDatabase.hpp"
#include <pqxx/pqxx>
#include <memory>
#include <string>
#include <vector>

namespace omnisphere::data
{
    /// PostgreSQL implementation using libpqxx 7+ (C++ wrapper over libpq).
    ///
    /// Transaction model:
    ///   - Without explicit Begin/Commit: each query runs in its own pqxx::work
    ///     that is committed immediately after the call.
    ///   - With explicit BeginTransaction(): a shared pqxx::work is kept alive
    ///     across calls until CommitTransaction() or RollbackTransaction().
    ///     On destruction or exception, pqxx auto-rolls back.
    class PostgreSQLDatabase : public IDatabase
    {
    private:
        std::unique_ptr<pqxx::connection> _conn;
        std::unique_ptr<pqxx::work>       _activeTxn; ///< Non-null only between Begin/Commit
        std::string _connectionString;

        /// Convert '?' placeholders to PostgreSQL positional '$N'.
        static std::string ConvertPlaceholders(const std::string& query);

        /// Map SQLParam variant → pqxx::params for exec_params().
        /// bool/int/double are appended as native types — no Y/N or string casting.
        static pqxx::params BuildParams(
            const std::vector<omnisphere::types::SQLParam>& params);

        /// Read a pqxx::result into a DataTable, dispatching column types by OID.
        omnisphere::types::DataTable ReadResult(const pqxx::result& res);

        /// Execute fn(pqxx::work&):
        ///   • If an explicit transaction is active → use it (caller commits/rolls back).
        ///   • Otherwise → create a one-shot pqxx::work, commit after fn returns.
        template<typename F>
        auto WithWork(F&& fn) -> std::invoke_result_t<F, pqxx::work&>;

    public:
        PostgreSQLDatabase();
        ~PostgreSQLDatabase() override;

        void ConnectionString(const std::string& cs) override;
        bool Connect()    override;
        void Disconnect() override;

        bool RunStatement(const std::string& query,
                          const std::string& context = "") override;
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

        bool BeginTransaction()    override;
        bool CommitTransaction()   override;
        bool RollbackTransaction() override;
    };

} // namespace omnisphere::data
