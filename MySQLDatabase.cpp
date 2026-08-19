#include "MySQLDatabase.hpp"
#include "SQLParams.hpp"
#include <OmniUtils/Logger.hpp>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <variant>

namespace omnisphere::data
{
    // ─── MySQL library lifetime (one per process) ─────────────────────────────
    namespace
    {
        struct MySQLLibrary
        {
            MySQLLibrary()  { mysql_library_init(0, nullptr, nullptr); }
            ~MySQLLibrary() { mysql_library_end(); }
        };
        static MySQLLibrary g_mysqlLibrary;
    }

    // ─── Logging helpers ──────────────────────────────────────────────────────
    static void LogSQL(const std::string& ctx, const std::string& q,
                       const std::vector<omnisphere::types::SQLParam>& p)
    {
        std::string prefix = ctx.empty() ? "" : "[" + ctx + "] ";
        omnisphere::utils::Logger::LogSQL("MySQL",
                                          prefix + omnisphere::types::FormatSQL(q, p));
    }
    static void LogSQL(const std::string& ctx, const std::string& q,
                       const std::vector<std::string>& p)
    {
        std::string prefix = ctx.empty() ? "" : "[" + ctx + "] ";
        omnisphere::utils::Logger::LogSQL("MySQL",
                                          prefix + omnisphere::types::FormatSQL(q, p));
    }
    static void LogSQL(const std::string& ctx, const std::string& q)
    {
        std::string prefix = ctx.empty() ? "" : "[" + ctx + "] ";
        omnisphere::utils::Logger::LogSQL("MySQL", prefix + q);
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    std::string MySQLDatabase::ExtractError() const
    {
        if (_conn) return mysql_error(_conn);
        return "[MySQLDatabase] No active connection.";
    }

    std::string MySQLDatabase::ExtractStmtError(MYSQL_STMT* stmt) const
    {
        if (stmt) return mysql_stmt_error(stmt);
        return "[MySQLDatabase] Null statement.";
    }

    /// Parse "host=h;port=p;dbname=d;user=u;password=pw;" format.
    void MySQLDatabase::ParseConnectionString(const std::string& cs)
    {
        std::istringstream ss(cs);
        std::string token;
        while (std::getline(ss, token, ';'))
        {
            auto pos = token.find('=');
            if (pos == std::string::npos) continue;
            std::string key = token.substr(0, pos);
            std::string val = token.substr(pos + 1);

            if      (key == "host")     _host     = val;
            else if (key == "port")     _port     = static_cast<unsigned int>(std::stoul(val));
            else if (key == "dbname")   _database = val;
            else if (key == "user")     _user     = val;
            else if (key == "password") _password = val;
        }
    }

    // ─── Build MYSQL_BIND[] for input parameters ──────────────────────────────
    //
    // Type mapping (C++ → MySQL native):
    //   std::monostate          → MYSQL_TYPE_NULL
    //   bool                    → MYSQL_TYPE_TINY  (native TINYINT/BOOLEAN, 0 or 1)
    //   int                     → MYSQL_TYPE_LONG
    //   double                  → MYSQL_TYPE_DOUBLE
    //   std::string             → MYSQL_TYPE_STRING
    //   std::vector<uint8_t>    → MYSQL_TYPE_BLOB
    //
    void MySQLDatabase::BuildParamBinds(
        const std::vector<omnisphere::types::SQLParam>& params,
        std::vector<ParamStorage>&                      storage,
        std::vector<MYSQL_BIND>&                        binds)
    {
        const size_t n = params.size();
        storage.resize(n);
        binds.resize(n);

        // Pre-size storage to prevent reallocation (pointers inside ParamStorage
        // are written into MYSQL_BIND; reallocation would invalidate them)
        // -> already done by resize(n) above.

        for (size_t i = 0; i < n; ++i)
        {
            std::memset(&binds[i], 0, sizeof(MYSQL_BIND));
            ParamStorage& s = storage[i];

            std::visit(
                [&](auto&& arg)
                {
                    using T = std::decay_t<decltype(arg)>;

                    if constexpr (std::is_same_v<T, std::monostate>)
                    {
                        s.isNull             = 1;
                        binds[i].buffer_type = MYSQL_TYPE_NULL;
                        binds[i].is_null     = &s.isNull;
                    }
                    else if constexpr (std::is_same_v<T, bool>)
                    {
                        // Native BOOLEAN — no Y/N conversion needed
                        s.i8                 = arg ? 1 : 0;
                        binds[i].buffer_type = MYSQL_TYPE_TINY;
                        binds[i].buffer      = &s.i8;
                        binds[i].buffer_length = sizeof(int8_t);
                        binds[i].is_null     = &s.isNull;
                    }
                    else if constexpr (std::is_same_v<T, int>)
                    {
                        s.i32                = arg;
                        binds[i].buffer_type = MYSQL_TYPE_LONG;
                        binds[i].buffer      = &s.i32;
                        binds[i].buffer_length = sizeof(int32_t);
                        binds[i].is_null     = &s.isNull;
                    }
                    else if constexpr (std::is_same_v<T, double>)
                    {
                        s.f64                = arg;
                        binds[i].buffer_type = MYSQL_TYPE_DOUBLE;
                        binds[i].buffer      = &s.f64;
                        binds[i].buffer_length = sizeof(double);
                        binds[i].is_null     = &s.isNull;
                    }
                    else if constexpr (std::is_same_v<T, std::string>)
                    {
                        s.str                = arg;
                        s.length             = static_cast<unsigned long>(s.str.size());
                        binds[i].buffer_type = MYSQL_TYPE_STRING;
                        binds[i].buffer      = const_cast<char*>(s.str.c_str());
                        binds[i].buffer_length = s.str.size();
                        binds[i].length      = &s.length;
                        binds[i].is_null     = &s.isNull;
                    }
                    else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
                    {
                        s.blob               = arg;
                        s.length             = static_cast<unsigned long>(s.blob.size());
                        binds[i].buffer_type = MYSQL_TYPE_BLOB;
                        binds[i].buffer      = s.blob.data();
                        binds[i].buffer_length = s.blob.size();
                        binds[i].length      = &s.length;
                        binds[i].is_null     = &s.isNull;
                    }
                },
                params[i]);
        }
    }

    // ─── FetchFromStatement ────────────────────────────────────────────────────
    //
    // Converts MYSQL_STMT results into a DataTable.
    //
    // Result type mapping (MySQL → DataTable::Row::Data):
    //   MYSQL_TYPE_TINY   (length==1)  → bool   (TINYINT(1) / BOOLEAN)
    //   MYSQL_TYPE_TINY   (length>1)   → int
    //   MYSQL_TYPE_SHORT/LONG/LONGLONG → int
    //   MYSQL_TYPE_FLOAT/DOUBLE        → double
    //   MYSQL_TYPE_DECIMAL/NEWDECIMAL  → double  (via string conversion)
    //   MYSQL_TYPE_BLOB  + BINARY_FLAG → vector<uint8_t>
    //   everything else               → string
    //
    omnisphere::types::DataTable MySQLDatabase::FetchFromStatement(MYSQL_STMT* stmt)
    {
        MYSQL_RES* meta = mysql_stmt_result_metadata(stmt);
        if (!meta) return omnisphere::types::DataTable{};

        int          numCols = mysql_num_fields(meta);
        MYSQL_FIELD* fields  = mysql_fetch_fields(meta);

        // Per-column storage: holds native-typed buffers that MYSQL_BIND points into.
        // Pre-size to prevent reallocation (pointers must stay valid).
        struct ColStorage
        {
            MYSQL_BIND    bind    = {};
            std::vector<char> buffer;   // for variable-length types
            unsigned long length = 0;
            my_bool       isNull = 0;
            my_bool       error  = 0;

            // Fixed-size native storage
            int8_t  i8  = 0;
            int32_t i32 = 0;
            int64_t i64 = 0;
            float   f32 = 0.0f;
            double  f64 = 0.0;
        };

        std::vector<ColStorage> cols(numCols);

        for (int i = 0; i < numCols; ++i)
        {
            MYSQL_FIELD& f  = fields[i];
            ColStorage&  cs = cols[i];

            cs.bind.is_null = &cs.isNull;
            cs.bind.error   = &cs.error;
            cs.bind.length  = &cs.length;

            switch (f.type)
            {
                case MYSQL_TYPE_TINY:
                    cs.bind.buffer_type   = MYSQL_TYPE_TINY;
                    cs.bind.buffer        = &cs.i8;
                    cs.bind.buffer_length = sizeof(int8_t);
                    break;

                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_YEAR:
                    cs.bind.buffer_type   = MYSQL_TYPE_LONG;
                    cs.bind.buffer        = &cs.i32;
                    cs.bind.buffer_length = sizeof(int32_t);
                    break;

                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_LONG:
                    cs.bind.buffer_type   = MYSQL_TYPE_LONG;
                    cs.bind.buffer        = &cs.i32;
                    cs.bind.buffer_length = sizeof(int32_t);
                    break;

                case MYSQL_TYPE_LONGLONG:
                    cs.bind.buffer_type   = MYSQL_TYPE_LONGLONG;
                    cs.bind.buffer        = &cs.i64;
                    cs.bind.buffer_length = sizeof(int64_t);
                    break;

                case MYSQL_TYPE_FLOAT:
                    cs.bind.buffer_type   = MYSQL_TYPE_FLOAT;
                    cs.bind.buffer        = &cs.f32;
                    cs.bind.buffer_length = sizeof(float);
                    break;

                case MYSQL_TYPE_DOUBLE:
                    cs.bind.buffer_type   = MYSQL_TYPE_DOUBLE;
                    cs.bind.buffer        = &cs.f64;
                    cs.bind.buffer_length = sizeof(double);
                    break;

                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                {
                    // DECIMAL arrives as string; allocate enough for max precision
                    unsigned long bufLen = (f.length > 0) ? f.length + 1 : 64;
                    cs.buffer.resize(bufLen);
                    cs.bind.buffer_type   = MYSQL_TYPE_STRING;
                    cs.bind.buffer        = cs.buffer.data();
                    cs.bind.buffer_length = cs.buffer.size();
                    break;
                }

                default:
                    // Strings, TEXT, BLOB, DATE, TIME, TIMESTAMP, ENUM, SET…
                    {
                        // cap initial alloc to 64 KB; handle truncation below
                        unsigned long bufLen = (f.length > 0 && f.length <= 65536)
                                                   ? f.length + 1
                                                   : 4096;
                        cs.buffer.resize(bufLen);
                        bool isBinary = (f.flags & BINARY_FLAG) &&
                                        (f.type == MYSQL_TYPE_BLOB       ||
                                         f.type == MYSQL_TYPE_TINY_BLOB  ||
                                         f.type == MYSQL_TYPE_MEDIUM_BLOB||
                                         f.type == MYSQL_TYPE_LONG_BLOB);
                        cs.bind.buffer_type   = isBinary ? MYSQL_TYPE_BLOB : MYSQL_TYPE_STRING;
                        cs.bind.buffer        = cs.buffer.data();
                        cs.bind.buffer_length = cs.buffer.size();
                    }
                    break;
            }
        }

        // Build a contiguous MYSQL_BIND array whose internal pointers reference
        // the (stable) ColStorage elements.
        std::vector<MYSQL_BIND> resultBinds(numCols);
        for (int i = 0; i < numCols; ++i)
            resultBinds[i] = cols[i].bind;

        if (mysql_stmt_bind_result(stmt, resultBinds.data()))
            throw std::runtime_error(
                std::string("[MySQLDatabase::FetchFromStatement] bind_result: ") +
                ExtractStmtError(stmt));

        // Store all rows in client memory (simplifies truncation handling)
        if (mysql_stmt_store_result(stmt))
            throw std::runtime_error(
                std::string("[MySQLDatabase::FetchFromStatement] store_result: ") +
                ExtractStmtError(stmt));

        std::vector<omnisphere::types::DataTable::Row> rows;

        int fetchRet;
        while ((fetchRet = mysql_stmt_fetch(stmt)) == 0 ||
               fetchRet == MYSQL_DATA_TRUNCATED)
        {
            omnisphere::types::DataTable::Row row;

            for (int i = 0; i < numCols; ++i)
            {
                ColStorage&  cs    = cols[i];
                MYSQL_FIELD& f     = fields[i];
                std::string  col   = f.name;

                if (cs.isNull)
                {
                    row.Set(col, std::nullopt);
                    continue;
                }

                // Handle truncation for variable-length columns
                if (fetchRet == MYSQL_DATA_TRUNCATED && cs.error)
                {
                    // Re-fetch this column with exact size
                    cs.buffer.resize(cs.length + 1);
                    resultBinds[i].buffer        = cs.buffer.data();
                    resultBinds[i].buffer_length = cs.buffer.size();
                    cs.error = 0;
                    mysql_stmt_fetch_column(stmt, &resultBinds[i],
                                            static_cast<unsigned int>(i), 0);
                }

                switch (f.type)
                {
                    case MYSQL_TYPE_TINY:
                        // TINYINT(1) / BOOLEAN → bool; TINYINT(n>1) → int
                        if (f.length == 1)
                            row.Set(col, cs.i8 != 0);
                        else
                            row.Set(col, static_cast<int>(cs.i8));
                        break;

                    case MYSQL_TYPE_SHORT:
                    case MYSQL_TYPE_YEAR:
                    case MYSQL_TYPE_INT24:
                    case MYSQL_TYPE_LONG:
                        row.Set(col, cs.i32);
                        break;

                    case MYSQL_TYPE_LONGLONG:
                        // Store as int (DataTable limitation for very large values)
                        row.Set(col, static_cast<int>(cs.i64));
                        break;

                    case MYSQL_TYPE_FLOAT:
                        row.Set(col, static_cast<double>(cs.f32));
                        break;

                    case MYSQL_TYPE_DOUBLE:
                        row.Set(col, cs.f64);
                        break;

                    case MYSQL_TYPE_DECIMAL:
                    case MYSQL_TYPE_NEWDECIMAL:
                        try { row.Set(col, std::stod(std::string(cs.buffer.data(), cs.length))); }
                        catch (...) { row.Set(col, std::nullopt); }
                        break;

                    default:
                    {
                        bool isBinary = (f.flags & BINARY_FLAG) &&
                                        (f.type == MYSQL_TYPE_BLOB       ||
                                         f.type == MYSQL_TYPE_TINY_BLOB  ||
                                         f.type == MYSQL_TYPE_MEDIUM_BLOB||
                                         f.type == MYSQL_TYPE_LONG_BLOB);
                        if (isBinary)
                        {
                            auto* ptr = reinterpret_cast<uint8_t*>(cs.buffer.data());
                            row.Set(col, std::vector<uint8_t>(ptr, ptr + cs.length));
                        }
                        else
                        {
                            row.Set(col, std::string(cs.buffer.data(), cs.length));
                        }
                        break;
                    }
                }
            }
            rows.push_back(std::move(row));
        }

        mysql_free_result(meta);

        omnisphere::types::DataTable table;
        table.Fill(rows);
        return table;
    }

    // ─── Constructor / Destructor ─────────────────────────────────────────────

    MySQLDatabase::MySQLDatabase()  = default;
    MySQLDatabase::~MySQLDatabase() { Disconnect(); }

    void MySQLDatabase::ConnectionString(const std::string& cs)
    {
        _connectionString = cs;
        ParseConnectionString(cs);
    }

    // ─── Connect / Disconnect ─────────────────────────────────────────────────

    bool MySQLDatabase::Connect()
    {
        if (_connectionString.empty())
            throw std::runtime_error("[MySQLDatabase::Connect] Connection string is empty.");

        // MySQL C API: one connection per thread
        mysql_thread_init();

        if (_conn)
        {
            mysql_close(_conn);
            _conn = nullptr;
        }

        _conn = mysql_init(nullptr);
        if (!_conn)
            throw std::runtime_error("[MySQLDatabase::Connect] mysql_init failed.");

        // Enable automatic reconnect
        my_bool reconnect = 1;
        mysql_options(_conn, MYSQL_OPT_RECONNECT, &reconnect);

        if (!mysql_real_connect(_conn,
                                _host.c_str(),
                                _user.c_str(),
                                _password.c_str(),
                                _database.empty() ? nullptr : _database.c_str(),
                                _port,
                                nullptr,  // unix socket
                                0))       // client flags
        {
            std::string err = ExtractError();
            mysql_close(_conn);
            _conn = nullptr;
            throw std::runtime_error("[MySQLDatabase::Connect] " + err);
        }

        // UTF-8 by default
        mysql_set_character_set(_conn, "utf8mb4");
        return true;
    }

    void MySQLDatabase::Disconnect()
    {
        if (_conn)
        {
            mysql_close(_conn);
            _conn = nullptr;
            mysql_thread_end();
        }
    }

    // ─── RunStatement ─────────────────────────────────────────────────────────

    bool MySQLDatabase::RunStatement(const std::string& query, const std::string& context)
    {
        LogSQL(context, query);
        if (!_conn)
            throw std::runtime_error("[MySQLDatabase::RunStatement] Not connected.");

        if (mysql_real_query(_conn, query.c_str(),
                             static_cast<unsigned long>(query.size())))
            throw std::runtime_error(
                std::string("[MySQLDatabase::RunStatement] ") + ExtractError());

        // Consume any result set (e.g. from stored procedures)
        if (MYSQL_RES* res = mysql_store_result(_conn))
            mysql_free_result(res);

        return true;
    }

    // ─── RunPrepared ──────────────────────────────────────────────────────────

    bool MySQLDatabase::RunPrepared(const std::string& query,
                                    const std::vector<omnisphere::types::SQLParam>& params,
                                    const std::string& context)
    {
        LogSQL(context, query, params);
        if (!_conn)
            throw std::runtime_error("[MySQLDatabase::RunPrepared] Not connected.");

        MYSQL_STMT* stmt = mysql_stmt_init(_conn);
        if (!stmt)
            throw std::runtime_error("[MySQLDatabase::RunPrepared] mysql_stmt_init failed.");

        try
        {
            if (mysql_stmt_prepare(stmt, query.c_str(),
                                   static_cast<unsigned long>(query.size())))
                throw std::runtime_error(
                    std::string("[MySQLDatabase::RunPrepared] prepare: ") +
                    ExtractStmtError(stmt));

            std::vector<ParamStorage> storage;
            std::vector<MYSQL_BIND>   binds;
            BuildParamBinds(params, storage, binds);

            if (!binds.empty() && mysql_stmt_bind_param(stmt, binds.data()))
                throw std::runtime_error(
                    std::string("[MySQLDatabase::RunPrepared] bind_param: ") +
                    ExtractStmtError(stmt));

            if (mysql_stmt_execute(stmt))
                throw std::runtime_error(
                    std::string("[MySQLDatabase::RunPrepared] execute: ") +
                    ExtractStmtError(stmt));

            mysql_stmt_close(stmt);
            return true;
        }
        catch (...)
        {
            mysql_stmt_close(stmt);
            throw;
        }
    }

    // ─── FetchPrepared (SQLParam) ─────────────────────────────────────────────

    omnisphere::types::DataTable MySQLDatabase::FetchPrepared(
        const std::string& query,
        const std::vector<omnisphere::types::SQLParam>& params,
        const std::string& context)
    {
        LogSQL(context, query, params);
        if (!_conn)
            throw std::runtime_error("[MySQLDatabase::FetchPrepared] Not connected.");

        MYSQL_STMT* stmt = mysql_stmt_init(_conn);
        if (!stmt)
            throw std::runtime_error("[MySQLDatabase::FetchPrepared] mysql_stmt_init failed.");

        try
        {
            if (mysql_stmt_prepare(stmt, query.c_str(),
                                   static_cast<unsigned long>(query.size())))
                throw std::runtime_error(
                    std::string("[MySQLDatabase::FetchPrepared] prepare: ") +
                    ExtractStmtError(stmt));

            std::vector<ParamStorage> storage;
            std::vector<MYSQL_BIND>   binds;
            BuildParamBinds(params, storage, binds);

            if (!binds.empty() && mysql_stmt_bind_param(stmt, binds.data()))
                throw std::runtime_error(
                    std::string("[MySQLDatabase::FetchPrepared] bind_param: ") +
                    ExtractStmtError(stmt));

            if (mysql_stmt_execute(stmt))
                throw std::runtime_error(
                    std::string("[MySQLDatabase::FetchPrepared] execute: ") +
                    ExtractStmtError(stmt));

            auto table = FetchFromStatement(stmt);
            mysql_stmt_close(stmt);
            return table;
        }
        catch (...)
        {
            mysql_stmt_close(stmt);
            throw;
        }
    }

    // ─── FetchPrepared (strings) ──────────────────────────────────────────────

    omnisphere::types::DataTable MySQLDatabase::FetchPrepared(
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

    omnisphere::types::DataTable MySQLDatabase::FetchPrepared(
        const std::string& query,
        const std::string& param,
        const std::string& context)
    {
        return FetchPrepared(query,
                             std::vector<omnisphere::types::SQLParam>{param},
                             context);
    }

    // ─── FetchResults ─────────────────────────────────────────────────────────

    omnisphere::types::DataTable MySQLDatabase::FetchResults(const std::string& query,
                                                              const std::string& context)
    {
        // Use prepared statement path for consistency (no params)
        return FetchPrepared(query, std::vector<omnisphere::types::SQLParam>{}, context);
    }

    // ─── Transactions ─────────────────────────────────────────────────────────

    bool MySQLDatabase::BeginTransaction()
    {
        try
        {
            if (mysql_autocommit(_conn, 0))
                throw std::runtime_error(ExtractError());
            return true;
        }
        catch (const std::exception& e)
        { throw std::runtime_error(std::string("[MySQLDatabase::BeginTransaction] ") + e.what()); }
    }

    bool MySQLDatabase::CommitTransaction()
    {
        try
        {
            if (mysql_commit(_conn))
                throw std::runtime_error(ExtractError());
            if (mysql_autocommit(_conn, 1))
                throw std::runtime_error(ExtractError());
            return true;
        }
        catch (const std::exception& e)
        { throw std::runtime_error(std::string("[MySQLDatabase::CommitTransaction] ") + e.what()); }
    }

    bool MySQLDatabase::RollbackTransaction()
    {
        try
        {
            if (mysql_rollback(_conn))
                throw std::runtime_error(ExtractError());
            if (mysql_autocommit(_conn, 1))
                throw std::runtime_error(ExtractError());
            return true;
        }
        catch (const std::exception& e)
        { throw std::runtime_error(std::string("[MySQLDatabase::RollbackTransaction] ") + e.what()); }
    }

} // namespace omnisphere::data
