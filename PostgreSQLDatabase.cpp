#include "PostgreSQLDatabase.hpp"
#include "SQLParams.hpp"
#include <OmniUtils/Logger.hpp>

#include <stdexcept>
#include <variant>

namespace omnisphere::data
{
    // ─── PostgreSQL native type OIDs ─────────────────────────────────────────
    // pqxx::oid is an alias for unsigned int (same underlying type as libpq Oid)
    static constexpr pqxx::oid OID_BOOL    = 16;
    static constexpr pqxx::oid OID_INT2    = 21;
    static constexpr pqxx::oid OID_INT4    = 23;
    static constexpr pqxx::oid OID_INT8    = 20;
    static constexpr pqxx::oid OID_FLOAT4  = 700;
    static constexpr pqxx::oid OID_FLOAT8  = 701;
    static constexpr pqxx::oid OID_NUMERIC = 1700;
    static constexpr pqxx::oid OID_BYTEA   = 17;
    // All other OIDs (TEXT, VARCHAR, DATE, TIMESTAMP, UUID, JSON…) → std::string

    // ─── Logging helpers ──────────────────────────────────────────────────────
    static void LogSQL(const std::string& ctx, const std::string& q,
                       const std::vector<omnisphere::types::SQLParam>& p)
    {
        std::string pfx = ctx.empty() ? "" : "[" + ctx + "] ";
        omnisphere::utils::Logger::LogSQL("PostgreSQL",
                                          pfx + omnisphere::types::FormatSQL(q, p));
    }
    static void LogSQL(const std::string& ctx, const std::string& q,
                       const std::vector<std::string>& p)
    {
        std::string pfx = ctx.empty() ? "" : "[" + ctx + "] ";
        omnisphere::utils::Logger::LogSQL("PostgreSQL",
                                          pfx + omnisphere::types::FormatSQL(q, p));
    }
    static void LogSQL(const std::string& ctx, const std::string& q)
    {
        std::string pfx = ctx.empty() ? "" : "[" + ctx + "] ";
        omnisphere::utils::Logger::LogSQL("PostgreSQL", pfx + q);
    }

    // ─── ConvertPlaceholders ──────────────────────────────────────────────────
    /// Converts ODBC-style '?' placeholders to PostgreSQL positional '$N'.
    /// Required because exec_params uses $1, $2, … in the query string.
    std::string PostgreSQLDatabase::ConvertPlaceholders(const std::string& query)
    {
        std::string result;
        result.reserve(query.size() + query.size() / 4);
        int idx = 1;
        for (char c : query)
        {
            if (c == '?')
            {
                result += '$';
                result += std::to_string(idx++);
            }
            else result += c;
        }
        return result;
    }

    // ─── BuildParams ─────────────────────────────────────────────────────────
    /// Map SQLParam → pqxx::params using native C++ types.
    ///
    /// Type mapping:
    ///   monostate       → NULL  (pqxx::params::append() with no arg)
    ///   bool            → bool  (native PostgreSQL BOOLEAN)
    ///   int             → int   (native PostgreSQL INT4)
    ///   double          → double (native PostgreSQL FLOAT8)
    ///   string          → string
    ///   vector<uint8_t> → pqxx::bytes_view (bytea)
    ///
    /// No manual string conversion needed — libpqxx handles type negotiation
    /// with the server using the binary protocol for numeric types.
    pqxx::params PostgreSQLDatabase::BuildParams(
        const std::vector<omnisphere::types::SQLParam>& params)
    {
        pqxx::params p;
        for (const auto& param : params)
        {
            std::visit(
                [&p](auto&& arg)
                {
                    using T = std::decay_t<decltype(arg)>;

                    if constexpr (std::is_same_v<T, std::monostate>)
                    {
                        p.append(); // NULL
                    }
                    else if constexpr (std::is_same_v<T, bool>)
                    {
                        p.append(arg); // native BOOLEAN — no 'Y'/'N' casting
                    }
                    else if constexpr (std::is_same_v<T, int>)
                    {
                        p.append(arg); // native INT4
                    }
                    else if constexpr (std::is_same_v<T, double>)
                    {
                        p.append(arg); // native FLOAT8 — full double precision
                    }
                    else if constexpr (std::is_same_v<T, std::string>)
                    {
                        p.append(arg);
                    }
                    else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
                    {
                        // bytea: pass as binary view — no hex encoding needed
                        p.append(pqxx::bytes_view{
                            reinterpret_cast<const std::byte*>(arg.data()),
                            arg.size()});
                    }
                },
                param);
        }
        return p;
    }

    // ─── ReadResult ───────────────────────────────────────────────────────────
    /// Convert a pqxx::result into a DataTable.
    /// Uses field.as<T>() for type-safe extraction — no manual OID→string→stoi.
    omnisphere::types::DataTable PostgreSQLDatabase::ReadResult(const pqxx::result& res)
    {
        std::vector<omnisphere::types::DataTable::Row> rows;
        rows.reserve(res.size());

        for (const auto& row : res)
        {
            omnisphere::types::DataTable::Row tableRow;

            for (const auto& field : row)
            {
                const std::string col = field.name();

                if (field.is_null())
                {
                    tableRow.Set(col, std::nullopt);
                    continue;
                }

                const pqxx::oid oid = field.type();

                if (oid == OID_BOOL)
                {
                    tableRow.Set(col, field.as<bool>());
                }
                else if (oid == OID_INT2 || oid == OID_INT4)
                {
                    tableRow.Set(col, field.as<int>());
                }
                else if (oid == OID_INT8)
                {
                    // DataTable stores int; large BIGINT values may truncate
                    tableRow.Set(col, static_cast<int>(field.as<long long>()));
                }
                else if (oid == OID_FLOAT4 || oid == OID_FLOAT8 || oid == OID_NUMERIC)
                {
                    tableRow.Set(col, field.as<double>());
                }
                else if (oid == OID_BYTEA)
                {
                    // pqxx handles unescaping automatically
                    const auto bytes = field.as<pqxx::bytes>();
                    tableRow.Set(col, std::vector<uint8_t>(
                        reinterpret_cast<const uint8_t*>(bytes.data()),
                        reinterpret_cast<const uint8_t*>(bytes.data() + bytes.size())));
                }
                else
                {
                    // TEXT, VARCHAR, DATE, TIMESTAMP, UUID, JSON, JSONB, ENUM, …
                    tableRow.Set(col, field.as<std::string>());
                }
            }

            rows.push_back(std::move(tableRow));
        }

        omnisphere::types::DataTable table;
        table.Fill(rows);
        return table;
    }

    // ─── WithWork ─────────────────────────────────────────────────────────────
    /// If an explicit transaction is active (BeginTransaction was called),
    /// execute fn within it — the caller owns commit/rollback.
    /// Otherwise, create a one-shot pqxx::work and commit immediately after fn.
    template<typename F>
    auto PostgreSQLDatabase::WithWork(F&& fn) -> std::invoke_result_t<F, pqxx::work&>
    {
        if (_activeTxn)
            return fn(*_activeTxn); // explicit transaction: do NOT commit here

        // One-shot: auto-rollback if fn throws (pqxx::work destructor)
        pqxx::work w(*_conn);
        auto result = fn(w);
        w.commit();
        return result;
    }

    // ─── Constructor / Destructor ─────────────────────────────────────────────
    PostgreSQLDatabase::PostgreSQLDatabase()  = default;
    PostgreSQLDatabase::~PostgreSQLDatabase() { Disconnect(); }

    void PostgreSQLDatabase::ConnectionString(const std::string& cs)
    {
        _connectionString = cs;
    }

    // ─── Connect / Disconnect ─────────────────────────────────────────────────
    bool PostgreSQLDatabase::Connect()
    {
        if (_connectionString.empty())
            throw std::runtime_error(
                "[PostgreSQLDatabase::Connect] Connection string is empty.");

        // pqxx::connection constructor throws pqxx::broken_connection on failure
        _conn = std::make_unique<pqxx::connection>(_connectionString);
        return true;
    }

    void PostgreSQLDatabase::Disconnect()
    {
        _activeTxn.reset(); // rollback any pending explicit transaction
        _conn.reset();      // closes the connection
    }

    // ─── RunStatement ─────────────────────────────────────────────────────────
    bool PostgreSQLDatabase::RunStatement(const std::string& query,
                                          const std::string& context)
    {
        LogSQL(context, query);
        if (!_conn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::RunStatement] Not connected.");

        return WithWork([&](pqxx::work& w)
        {
            w.exec(query); // pqxx::work::exec throws pqxx::sql_error on failure
            return true;
        });
    }

    // ─── RunPrepared ──────────────────────────────────────────────────────────
    bool PostgreSQLDatabase::RunPrepared(const std::string& query,
                                         const std::vector<omnisphere::types::SQLParam>& params,
                                         const std::string& context)
    {
        LogSQL(context, query, params);
        if (!_conn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::RunPrepared] Not connected.");

        const std::string pgQuery = ConvertPlaceholders(query);
        const pqxx::params p      = BuildParams(params);

        return WithWork([&](pqxx::work& w)
        {
            w.exec(pgQuery, p);
            return true;
        });
    }

    // ─── FetchPrepared (SQLParam) ─────────────────────────────────────────────
    omnisphere::types::DataTable PostgreSQLDatabase::FetchPrepared(
        const std::string& query,
        const std::vector<omnisphere::types::SQLParam>& params,
        const std::string& context)
    {
        LogSQL(context, query, params);
        if (!_conn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::FetchPrepared] Not connected.");

        const std::string pgQuery = ConvertPlaceholders(query);
        const pqxx::params p      = BuildParams(params);

        return WithWork([&](pqxx::work& w)
        {
            return ReadResult(w.exec(pgQuery, p));
        });
    }

    // ─── FetchPrepared (vector<string>) ──────────────────────────────────────
    omnisphere::types::DataTable PostgreSQLDatabase::FetchPrepared(
        const std::string& query,
        const std::vector<std::string>& params,
        const std::string& context)
    {
        std::vector<omnisphere::types::SQLParam> converted;
        converted.reserve(params.size());
        for (const auto& s : params) converted.push_back(s);
        return FetchPrepared(query, converted, context);
    }

    // ─── FetchPrepared (single string) ────────────────────────────────────────
    omnisphere::types::DataTable PostgreSQLDatabase::FetchPrepared(
        const std::string& query,
        const std::string& param,
        const std::string& context)
    {
        return FetchPrepared(query,
                             std::vector<omnisphere::types::SQLParam>{param},
                             context);
    }

    // ─── FetchResults (no params) ─────────────────────────────────────────────
    omnisphere::types::DataTable PostgreSQLDatabase::FetchResults(
        const std::string& query,
        const std::string& context)
    {
        LogSQL(context, query);
        if (!_conn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::FetchResults] Not connected.");

        return WithWork([&](pqxx::work& w)
        {
            return ReadResult(w.exec(query));
        });
    }

    // ─── Transactions ─────────────────────────────────────────────────────────
    /// Creates a pqxx::work transaction. libpqxx automatically sends BEGIN
    /// to PostgreSQL. The transaction lives until Commit or Rollback is called.
    bool PostgreSQLDatabase::BeginTransaction()
    {
        if (!_conn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::BeginTransaction] Not connected.");
        if (_activeTxn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::BeginTransaction] Already in a transaction.");

        _activeTxn = std::make_unique<pqxx::work>(*_conn);
        return true;
    }

    bool PostgreSQLDatabase::CommitTransaction()
    {
        if (!_activeTxn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::CommitTransaction] No active transaction.");

        _activeTxn->commit(); // sends COMMIT
        _activeTxn.reset();
        return true;
    }

    bool PostgreSQLDatabase::RollbackTransaction()
    {
        if (!_activeTxn)
            throw std::runtime_error(
                "[PostgreSQLDatabase::RollbackTransaction] No active transaction.");

        // pqxx::work destructor sends ROLLBACK automatically,
        // but calling abort() is more explicit.
        _activeTxn->abort();
        _activeTxn.reset();
        return true;
    }

} // namespace omnisphere::data
