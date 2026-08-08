#include "PostgreSQLDatabase.hpp"
#include <stdexcept>
#include <vector>
#include <map>
#include <OmniUtils/Logger.hpp>
#include <variant>
#include "SQLParams.hpp"

namespace omnisphere::data
{
    static void LogSQL(const std::string& context, const std::string& query, const std::vector<omnisphere::types::SQLParam>& params)
    {
        std::string ctxStr = context.empty() ? "" : "[" + context + "] ";
        std::string formattedQuery = omnisphere::types::FormatSQL(query, params);
        omnisphere::utils::Logger::LogSQL("PostgreSQL", ctxStr + formattedQuery);
    }

    static void LogSQL(const std::string& context, const std::string& query, const std::vector<std::string>& params)
    {
        std::string ctxStr = context.empty() ? "" : "[" + context + "] ";
        std::string formattedQuery = omnisphere::types::FormatSQL(query, params);
        omnisphere::utils::Logger::LogSQL("PostgreSQL", ctxStr + formattedQuery);
    }

    static void LogSQL(const std::string& context, const std::string& query)
    {
        std::string ctxStr = context.empty() ? "" : "[" + context + "] ";
        omnisphere::utils::Logger::LogSQL("PostgreSQL", ctxStr + query);
    }

    std::string PostgreSQLDatabase::ExtractError(const char *fn, SQLHANDLE handle,
                                                SQLSMALLINT type)
    {
        if (!handle)
        {
            return std::string(" [ExtractError] Null Handle or not initialized.");
        }

        SQLINTEGER i = 0;
        SQLINTEGER native;
        SQLCHAR state[7];
        SQLCHAR text[256];
        SQLSMALLINT len;
        SQLRETURN ret;

        std::string errors;
        do
        {
            ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text),
                                &len);

            if (SQL_SUCCEEDED(ret))
            {
                errors += "SQL State: ";
                errors += reinterpret_cast<const char *>(state);
                errors += "\nMessage: ";
                errors += std::string(reinterpret_cast<const char *>(text), len);
            }
            else if (ret == SQL_INVALID_HANDLE)
            {
                errors += "[ExtractError] SQL_INVALID_HANDLE";
                break;
            }
        } while (ret == SQL_SUCCESS);

        return errors;
    }

    PostgreSQLDatabase::PostgreSQLDatabase()
        : henv(SQL_NULL_HENV), hdbc(SQL_NULL_HDBC), hstmt(SQL_NULL_HSTMT) {}

    PostgreSQLDatabase::~PostgreSQLDatabase()
    {
        Disconnect();
    }

    void PostgreSQLDatabase::ConnectionString(const std::string &connectionString)
    {
        this->_ConnectionString = connectionString;
    }

    bool PostgreSQLDatabase::Connect()
    {
        if (_ConnectionString.empty())
        {
            throw std::runtime_error("[PostgreSQLDatabase::Connect] Connection string is empty.");
        }

        try
        {
            SQLRETURN ret;

            std::string connString = _ConnectionString;

            ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLAllocHandle ENV", henv, SQL_HANDLE_ENV));

            ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLSetEnvAttr", henv, SQL_HANDLE_ENV));

            ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLAllocHandle DBC", henv, SQL_HANDLE_ENV));

            ret = SQLDriverConnect(hdbc, nullptr, (SQLCHAR *)connString.c_str(),
                                   SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLConnect", hdbc, SQL_HANDLE_DBC));

            return true;
        }
        catch (const std::exception &e)
        {
            throw std::runtime_error(std::string("[PostgreSQLDatabase::Connect] ") + e.what());
        }
    }

    void PostgreSQLDatabase::Disconnect()
    {
        try
        {
            if (hdbc != SQL_NULL_HDBC)
            {
                SQLDisconnect(hdbc);
                SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
                hdbc = SQL_NULL_HDBC;
            }

            if (henv != SQL_NULL_HENV)
            {
                SQLFreeHandle(SQL_HANDLE_ENV, henv);
                henv = SQL_NULL_HENV;
            }
        }
        catch (const std::exception &e)
        {
            throw std::runtime_error(
                std::string("[PostgreSQLDatabase::Disconnect] ") + e.what() + "\n" +
                ExtractError("PostgreSQLDatabase::Disconnect", hdbc, SQL_HANDLE_DBC));
        }
    }

    void PostgreSQLDatabase::PrepareStatement(const std::string &query)
    {
        try
        {
            SQLRETURN ret;

            if (hstmt != SQL_NULL_HSTMT)
            {
                SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
                hstmt = SQL_NULL_HSTMT;
            }

            ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLAllocHandle STMT", hdbc, SQL_HANDLE_DBC));

            ret = SQLPrepare(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLPrepare", hstmt, SQL_HANDLE_STMT));
        }
        catch (const std::exception &e)
        {
            throw std::runtime_error(std::string("[PostgreSQLDatabase::PrepareStatement] ") + e.what());
        }
    }

    bool PostgreSQLDatabase::RunStatement(const std::string &query, const std::string& context)
    {
        LogSQL(context, query);
        try
        {
            SQLRETURN ret;
            PrepareStatement(query);

            ret = SQLExecute(hstmt);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLExecute", hstmt, SQL_HANDLE_STMT));

            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            hstmt = SQL_NULL_HSTMT;

            return true;
        }
        catch (const std::exception &e)
        {
            if (hstmt != SQL_NULL_HSTMT)
            {
                SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
                hstmt = SQL_NULL_HSTMT;
            }
            throw std::runtime_error(std::string("[PostgreSQLDatabase::RunStatement] ") + e.what());
        }
    }

    bool PostgreSQLDatabase::RunPrepared(const std::string &query,
                                       const std::vector<omnisphere::types::SQLParam> &params,
                                       const std::string& context)
    {
        LogSQL(context, query, params);
        try
        {
            SQLRETURN ret;
            PrepareStatement(query);

            doubleStorage.clear();
            stringStorage.clear();
            binaryStorage.clear();
            intStorage.clear();
            indStorage.clear();

            for (const auto &p : params)
            {
                if (std::holds_alternative<double>(p))
                    doubleStorage.push_back(std::get<double>(p));
                else if (std::holds_alternative<std::string>(p))
                    stringStorage.push_back(std::get<std::string>(p));
                else if (std::holds_alternative<std::vector<uint8_t>>(p))
                    binaryStorage.push_back(std::get<std::vector<uint8_t>>(p));
                else if (std::holds_alternative<int>(p))
                    intStorage.push_back(std::get<int>(p));
            }

            doubleStorage.reserve(params.size());
            stringStorage.reserve(params.size());
            binaryStorage.reserve(params.size());
            intStorage.reserve(params.size());
            indStorage.resize(params.size(), 0);

            size_t dIdx = 0, sIdx = 0, bIdx = 0, iIdx = 0;

            for (size_t i = 0; i < params.size(); ++i)
            {
                SQLUSMALLINT paramNum = static_cast<SQLUSMALLINT>(i + 1);
                const auto &param = params[i];

                std::visit(
                    [&](auto &&arg) {
                        using T = std::decay_t<decltype(arg)>;

                        if constexpr (std::is_same_v<T, std::monostate>)
                        {
                            indStorage[i] = SQL_NULL_DATA;
                            ret = SQLBindParameter(hstmt, paramNum, SQL_PARAM_INPUT,
                                                   SQL_C_CHAR, SQL_VARCHAR, 0, 0, nullptr,
                                                   0, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, int>)
                        {
                            indStorage[i] = 0;
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                                0, 0, &intStorage[iIdx++], 0, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, double>)
                        {
                            indStorage[i] = 0;
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE,
                                0, 0, &doubleStorage[dIdx++], 0, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, std::string>)
                        {
                            indStorage[i] = SQL_NTS;
                            const std::string &str = stringStorage[sIdx++];
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                                str.length(), 0, (SQLPOINTER)str.c_str(), str.length() + 1, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
                        {
                            const auto &vec = binaryStorage[bIdx++];
                            indStorage[i] = vec.size();
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_BINARY,
                                SQL_VARBINARY, vec.size(), 0, (SQLPOINTER)vec.data(),
                                vec.size(), &indStorage[i]);
                        }
                    },
                    param);

                if (!SQL_SUCCEEDED(ret))
                {
                    throw std::runtime_error(
                        ExtractError("SQLBindParameter", hstmt, SQL_HANDLE_STMT));
                }
            }

            ret = SQLExecute(hstmt);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLExecute", hstmt, SQL_HANDLE_STMT));

            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            hstmt = SQL_NULL_HSTMT;

            return true;
        }
        catch (const std::exception &e)
        {
            if (hstmt != SQL_NULL_HSTMT)
            {
                SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
                hstmt = SQL_NULL_HSTMT;
            }
            throw std::runtime_error(std::string("[PostgreSQLDatabase::RunPrepared] ") + e.what());
        }
    }

    omnisphere::types::DataTable PostgreSQLDatabase::FetchPrepared(
        const std::string &query, const std::vector<omnisphere::types::SQLParam> &params, const std::string& context)
    {
        LogSQL(context, query, params);
        try
        {
            SQLRETURN ret;
            PrepareStatement(query);

            doubleStorage.clear();
            stringStorage.clear();
            binaryStorage.clear();
            intStorage.clear();
            indStorage.clear();

            for (const auto &p : params)
            {
                if (std::holds_alternative<double>(p))
                    doubleStorage.push_back(std::get<double>(p));
                else if (std::holds_alternative<std::string>(p))
                    stringStorage.push_back(std::get<std::string>(p));
                else if (std::holds_alternative<std::vector<uint8_t>>(p))
                    binaryStorage.push_back(std::get<std::vector<uint8_t>>(p));
                else if (std::holds_alternative<int>(p))
                    intStorage.push_back(std::get<int>(p));
            }

            doubleStorage.reserve(params.size());
            stringStorage.reserve(params.size());
            binaryStorage.reserve(params.size());
            intStorage.reserve(params.size());
            indStorage.resize(params.size(), 0);

            size_t dIdx = 0, sIdx = 0, bIdx = 0, iIdx = 0;

            for (size_t i = 0; i < params.size(); ++i)
            {
                SQLUSMALLINT paramNum = static_cast<SQLUSMALLINT>(i + 1);
                const auto &param = params[i];

                std::visit(
                    [&](auto &&arg) {
                        using T = std::decay_t<decltype(arg)>;

                        if constexpr (std::is_same_v<T, std::monostate>)
                        {
                            indStorage[i] = SQL_NULL_DATA;
                            ret = SQLBindParameter(hstmt, paramNum, SQL_PARAM_INPUT,
                                                   SQL_C_CHAR, SQL_VARCHAR, 0, 0, nullptr,
                                                   0, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, int>)
                        {
                            indStorage[i] = 0;
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
                                0, 0, &intStorage[iIdx++], 0, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, double>)
                        {
                            indStorage[i] = 0;
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE,
                                0, 0, &doubleStorage[dIdx++], 0, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, std::string>)
                        {
                            indStorage[i] = SQL_NTS;
                            const std::string &str = stringStorage[sIdx++];
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                                str.length(), 0, (SQLPOINTER)str.c_str(), str.length() + 1, &indStorage[i]);
                        }
                        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
                        {
                            const auto &vec = binaryStorage[bIdx++];
                            indStorage[i] = vec.size();
                            ret = SQLBindParameter(
                                hstmt, paramNum, SQL_PARAM_INPUT, SQL_C_BINARY,
                                SQL_VARBINARY, vec.size(), 0, (SQLPOINTER)vec.data(),
                                vec.size(), &indStorage[i]);
                        }
                    },
                    param);

                if (!SQL_SUCCEEDED(ret))
                {
                    throw std::runtime_error(
                        ExtractError("SQLBindParameter", hstmt, SQL_HANDLE_STMT));
                }
            }

            ret = SQLExecute(hstmt);

            if (!SQL_SUCCEEDED(ret))
                throw std::runtime_error(
                    ExtractError("SQLExecute", hstmt, SQL_HANDLE_STMT));

            SQLSMALLINT columnCount;
            SQLNumResultCols(hstmt, &columnCount);

            std::vector<std::string> columnNames(columnCount);
            std::vector<SQLSMALLINT> nativeTypes(columnCount);

            SQLCHAR columnName[256];
            SQLSMALLINT columnNameLength, decimalDigits, nullable;
            SQLULEN columnSize;

            for (SQLUSMALLINT i = 1; i <= columnCount; ++i)
            {
                SQLDescribeCol(hstmt, i, columnName, sizeof(columnName),
                               &columnNameLength, &nativeTypes[i - 1], &columnSize,
                               &decimalDigits, &nullable);
                columnNames[i - 1] =
                std::string(reinterpret_cast<char *>(columnName), columnNameLength);
            }

            omnisphere::types::DataTable table;
            std::vector<omnisphere::types::DataTable::Row> rows;

            while ((ret = SQLFetch(hstmt)) == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
            {
                omnisphere::types::DataTable::Row row;

                for (SQLUSMALLINT i = 1; i <= columnCount; ++i)
                {
                    SQLLEN indicator = 0;
                    SQLSMALLINT nativeType = nativeTypes[i - 1];
                    const std::string &colName = columnNames[i - 1];

                    if (nativeType == SQL_VARBINARY || nativeType == SQL_BINARY)
                    {
                        std::vector<uint8_t> buffer(512);
                        SQLRETURN retData = SQLGetData(hstmt, i, SQL_C_BINARY, buffer.data(),
                                                   (SQLLEN)buffer.size(), &indicator);

                        if (retData == SQL_SUCCESS || retData == SQL_SUCCESS_WITH_INFO)
                        {
                            if (indicator == SQL_NULL_DATA)
                            {
                                row.Set(colName, std::nullopt);
                            }
                            else
                            {
                                size_t size = (indicator > 0 &&
                                               indicator < static_cast<SQLLEN>(buffer.size()))
                                ? static_cast<size_t>(indicator)
                                    : buffer.size();
                                buffer.resize(size);
                                row.Set(colName, buffer);
                            }
                        }
                    }
                    else if (nativeType == SQL_INTEGER || nativeType == SQL_SMALLINT ||
                             nativeType == SQL_TINYINT)
                    {
                        int valInt = 0;
                        SQLRETURN retData = SQLGetData(hstmt, i, SQL_C_SLONG, &valInt, 0, &indicator);

                        if (retData == SQL_SUCCESS || retData == SQL_SUCCESS_WITH_INFO)
                        {
                            if (indicator == SQL_NULL_DATA)
                            {
                                row.Set(colName, std::nullopt);
                            }
                            else
                            {
                                row.Set(colName, valInt);
                            }
                        }
                    }
                    else if (nativeType == SQL_DECIMAL || nativeType == SQL_NUMERIC ||
                             nativeType == SQL_REAL || nativeType == SQL_FLOAT ||
                             nativeType == SQL_DOUBLE)
                    {
                        double valDouble = 0.0;
                        SQLRETURN retData = SQLGetData(hstmt, i, SQL_C_DOUBLE, &valDouble, 0, &indicator);

                        if (retData == SQL_SUCCESS || retData == SQL_SUCCESS_WITH_INFO)
                        {
                            if (indicator == SQL_NULL_DATA)
                            {
                                row.Set(colName, std::nullopt);
                            }
                            else
                            {
                                row.Set(colName, valDouble);
                            }
                        }
                    }
                    else
                    {
                        char buffer[1024] = {0};
                        SQLRETURN retData = SQLGetData(hstmt, i, SQL_C_CHAR, buffer,
                                                   sizeof(buffer), &indicator);

                        if (retData == SQL_SUCCESS || retData == SQL_SUCCESS_WITH_INFO)
                        {
                            if (indicator == SQL_NULL_DATA)
                            {
                                row.Set(colName, std::nullopt);
                            }
                            else
                            {
                                std::string valStr(buffer);
                                row.Set(colName, valStr);
                            }
                        }
                    }
                }

                rows.push_back(std::move(row));
            }

            table.Fill(rows);

            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            hstmt = SQL_NULL_HSTMT;

            return table;
        }
        catch (const std::exception &e)
        {
            if (hstmt != SQL_NULL_HSTMT)
            {
                SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
                hstmt = SQL_NULL_HSTMT;
            }
            throw std::runtime_error(std::string("[PostgreSQLDatabase::FetchPrepared] ") + e.what());
        }
    }

    omnisphere::types::DataTable PostgreSQLDatabase::FetchPrepared(
        const std::string &query, const std::vector<std::string> &params, const std::string& context)
    {
        std::vector<omnisphere::types::SQLParam> convertedParams;
        for (const auto &p : params)
            convertedParams.push_back(p);
        return FetchPrepared(query, convertedParams, context);
    }

    omnisphere::types::DataTable PostgreSQLDatabase::FetchPrepared(
        const std::string &query, const std::string &param, const std::string& context)
    {
        std::vector<omnisphere::types::SQLParam> params = {param};
        return FetchPrepared(query, params, context);
    }

    omnisphere::types::DataTable PostgreSQLDatabase::FetchResults(const std::string &query, const std::string& context)
    {
        return FetchPrepared(query, std::vector<omnisphere::types::SQLParam>{}, context);
    }

    bool PostgreSQLDatabase::BeginTransaction()
    {
        try
        {
            return RunStatement("BEGIN TRANSACTION", "PostgreSQLDatabase::BeginTransaction");
        }
        catch (const std::exception &e)
        {
            throw std::runtime_error(std::string("[PostgreSQLDatabase::BeginTransaction] ") + e.what());
        }
    }

    bool PostgreSQLDatabase::CommitTransaction()
    {
        try
        {
            return RunStatement("COMMIT", "PostgreSQLDatabase::CommitTransaction");
        }
        catch (const std::exception &e)
        {
            throw std::runtime_error(std::string("[PostgreSQLDatabase::CommitTransaction] ") + e.what());
        }
    }

    bool PostgreSQLDatabase::RollbackTransaction()
    {
        try
        {
            return RunStatement("ROLLBACK", "PostgreSQLDatabase::RollbackTransaction");
        }
        catch (const std::exception &e)
        {
            throw std::runtime_error(std::string("[PostgreSQLDatabase::RollbackTransaction] ") + e.what());
        }
    }

} // namespace omnisphere::data
