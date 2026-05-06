#include <SQLServerDatabase.hpp>
#include <stdexcept>
#include <vector>

namespace omnisphere::services {

std::string SQLServerDatabase::ExtractError(const char *fn, SQLHANDLE handle,
                                           SQLSMALLINT type) {
  if (!handle) {
    return std::string(" [ExtractError] Null Handle or not initialized.");
  }

  SQLINTEGER i = 0;
  SQLINTEGER native;
  SQLCHAR state[7];
  SQLCHAR text[256];
  SQLSMALLINT len;
  SQLRETURN ret;

  std::string errors;
  do {
    ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text),
                        &len);
    if (SQL_SUCCEEDED(ret)) {
      errors += "SQL State: ";
      errors += reinterpret_cast<const char *>(state);
      errors += "\nMessage: ";
      errors += std::string(reinterpret_cast<const char *>(text), len);
    } else if (ret == SQL_INVALID_HANDLE) {
      errors += "[ExtractError] SQL_INVALID_HANDLE";
      break;
    }
  } while (ret == SQL_SUCCESS);

  return errors;
}

SQLServerDatabase::SQLServerDatabase()
    : henv(SQL_NULL_HENV), hdbc(SQL_NULL_HDBC), hstmt(SQL_NULL_HSTMT) {}

SQLServerDatabase::~SQLServerDatabase() { Disconnect(); }

void SQLServerDatabase::ConnectionString(const std::string &connectionString) {
  this->_ConnectionString = connectionString;
}

bool SQLServerDatabase::Connect() {
  if (_ConnectionString.empty()) {
    throw std::runtime_error("[SQLServerDatabase::Connect] Connection string is empty.");
  }

  try {
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
  } catch (const std::exception &e) {
    throw std::runtime_error(std::string("[SQLServerDatabase::Connect] ") + e.what());
  }
}

void SQLServerDatabase::Disconnect() {
  try {
    if (hdbc != SQL_NULL_HDBC) {
      SQLDisconnect(hdbc);
      SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
      hdbc = SQL_NULL_HDBC;
    }
    if (henv != SQL_NULL_HENV) {
      SQLFreeHandle(SQL_HANDLE_ENV, henv);
      henv = SQL_NULL_HENV;
    }
  } catch (const std::exception &e) {
    throw std::runtime_error(
        std::string("[SQLServerDatabase::Disconnect] ") + e.what() + "\n" +
        ExtractError("SQLServerDatabase::Disconnect", hdbc, SQL_HANDLE_DBC));
  }
}

void SQLServerDatabase::PrepareStatement(const std::string &query) {
  try {
    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLAllocHandle", hdbc, SQL_HANDLE_DBC));

    ret = SQLPrepare(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLPrepare", hstmt, SQL_HANDLE_STMT));
  } catch (const std::exception &ex) {
    throw std::runtime_error(std::string("[SQLServerDatabase::PrepareStatement] ") +
                             ex.what());
  }
}

bool SQLServerDatabase::RunStatement(const std::string &query) {
  try {
    SQLRETURN ret;

    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLAllocHandle", hdbc, SQL_HANDLE_DBC));

    ret = SQLExecDirect(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLServerDatabase::RunStatement", hstmt, SQL_HANDLE_STMT) +
          query + "\n");

    return true;
  } catch (const std::exception &e) {
    throw std::runtime_error(std::string("[SQLServerDatabase::RunStatement] ") +
                             e.what());
  }
}

bool SQLServerDatabase::BeginTransaction() {
  try {
    SQLRETURN ret = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
                                      (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLServerDatabase::BeginTransaction", hdbc, SQL_HANDLE_DBC));

    return true;
  } catch (const std::exception &e) {
    throw std::runtime_error(std::string("[SQLServerDatabase::BeginTransaction] ") +
                             e.what());
  }
}

bool SQLServerDatabase::CommitTransaction() {
  try {
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("Commit Error", hdbc, SQL_HANDLE_DBC));

    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
                            (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLServerDatabase::Commit", hdbc, SQL_HANDLE_DBC));

    return true;
  } catch (const std::exception &e) {
    throw std::runtime_error(std::string("[SQLServerDatabase::Commit] ") + e.what());
  }
}

bool SQLServerDatabase::RollbackTransaction() {
  try {
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLServerDatabase::Rollback", hdbc, SQL_HANDLE_DBC));

    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
                            (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);

    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLServerDatabase::Rollback", hdbc, SQL_HANDLE_DBC));

    return true;
  } catch (const std::exception &e) {
    throw std::runtime_error(std::string("[SQLServerDatabase::Rollback] ") + e.what());
  }
}

bool SQLServerDatabase::RunPrepared(
    const std::string &query,
    const std::vector<omnisphere::types::SQLParam> &params) {
  try {
    SQLRETURN retcode;

    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }

    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

    if (!SQL_SUCCEEDED(retcode))
      throw std::runtime_error(
          ExtractError("SQLAllocHandle", hdbc, SQL_HANDLE_DBC));

    retcode = SQLPrepare(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);

    if (!SQL_SUCCEEDED(retcode))
      throw std::runtime_error(
          ExtractError("SQLPrepare", hstmt, SQL_HANDLE_STMT));

    stringStorage.clear();
    stringStorage.reserve(params.size());

    intStorage.clear();
    intStorage.reserve(params.size());

    binaryStorage.clear();
    binaryStorage.reserve(params.size());

    indStorage.clear();
    indStorage.resize(params.size());

    doubleStorage.clear();
    doubleStorage.resize(params.size());

    SQLUSMALLINT paramIndex = 1;

    struct ParamVisitor {
      SQLHSTMT hstmt;
      SQLUSMALLINT paramIndex;
      std::vector<SQLLEN> &indStorage;
      std::vector<int> &intStorage;
      std::vector<double> &doubleStorage;
      std::vector<std::string> &stringStorage;
      std::vector<std::vector<uint8_t>> &binaryStorage;

      SQLRETURN operator()(const std::monostate &) const {
        static char dummy = 0;
        indStorage[paramIndex - 1] = SQL_NULL_DATA;
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, 0, 0, &dummy, 0,
                                &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const int &value) const {
        intStorage.push_back(value);
        indStorage[paramIndex - 1] = sizeof(int);
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_SLONG,
                                SQL_INTEGER, 0, 0, &intStorage.back(), 0,
                                &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const double &value) const {
        doubleStorage.push_back(value);
        indStorage[paramIndex - 1] = sizeof(double);
        return SQLBindParameter(
            hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0,
            &doubleStorage.back(), 0, &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const std::string &value) const {
        stringStorage.emplace_back(value);
        std::string &storedStr = stringStorage.back();
        indStorage[paramIndex - 1] = static_cast<SQLLEN>(storedStr.size());
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, indStorage[paramIndex - 1], 0,
                                (SQLPOINTER)storedStr.c_str(), 0,
                                &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const std::vector<uint8_t> &value) const {
        binaryStorage.push_back(value);
        std::vector<uint8_t> &storedBin = binaryStorage.back();
        indStorage[paramIndex - 1] = static_cast<SQLLEN>(storedBin.size());
        return SQLBindParameter(
            hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY,
            indStorage[paramIndex - 1], 0, (SQLPOINTER)storedBin.data(),
            indStorage[paramIndex - 1], &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const bool &value) const {
        std::string ynValue = value ? "Y" : "N";
        stringStorage.emplace_back(ynValue);
        std::string &storedStr = stringStorage.back();
        indStorage[paramIndex - 1] = static_cast<SQLLEN>(storedStr.size());
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, indStorage[paramIndex - 1], 0,
                                (SQLPOINTER)storedStr.c_str(), 0,
                                &indStorage[paramIndex - 1]);
      }
    };

    for (const omnisphere::types::SQLParam &param : params) {
      ParamVisitor visitor{hstmt,        paramIndex,    indStorage,
                           intStorage,   doubleStorage, stringStorage,
                           binaryStorage};
      retcode = std::visit(visitor, param);

      if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Error binding parameter " +
                                 std::to_string(paramIndex));
      }

      paramIndex++;
    }
    retcode = SQLExecute(hstmt);

    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
      throw std::runtime_error(
          ExtractError("SQLExecute", hstmt, SQL_HANDLE_STMT));

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    hstmt = SQL_NULL_HSTMT;

    return true;
  } catch (const std::exception &ex) {
    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }
    throw std::runtime_error(std::string("[SQLServerDatabase::RunPrepared Exception] ") +
                             ex.what());
  }
}

omnisphere::types::DataTable SQLServerDatabase::FetchResults(const std::string &query) {
  omnisphere::types::DataTable dataTable;

  try {
    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLAllocHandle", hdbc, SQL_HANDLE_DBC));

    SQLRETURN retcode = SQLExecDirect(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
      throw std::runtime_error(
          ExtractError("SQLExecDirect", hstmt, SQL_HANDLE_STMT));

    SQLSMALLINT columnCount;
    SQLNumResultCols(hstmt, &columnCount);

    std::vector<std::string> columnNames(columnCount);
    std::vector<SQLSMALLINT> nativeTypes(columnCount);

    SQLCHAR columnName[256];
    SQLSMALLINT columnNameLength, dataType, decimalDigits, nullable;
    SQLULEN columnSize;

    for (SQLUSMALLINT i = 1; i <= columnCount; ++i) {
      SQLDescribeCol(hstmt, i, columnName, sizeof(columnName),
                     &columnNameLength, &nativeTypes[i - 1], &columnSize,
                     &decimalDigits, &nullable);

      columnNames[i - 1] =
          std::string(reinterpret_cast<char *>(columnName), columnNameLength);
    }

    std::vector<omnisphere::types::DataTable::Row> rows;

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
      omnisphere::types::DataTable::Row row;
      for (SQLUSMALLINT i = 1; i <= columnCount; ++i) {
        SQLLEN indicator = 0;
        SQLSMALLINT nativeType = nativeTypes[i - 1];
        const std::string &colName = columnNames[i - 1];

        try {
          if (nativeType == SQL_VARBINARY || nativeType == SQL_BINARY) {
            std::vector<uint8_t> buffer(512);
            SQLRETURN ret = SQLGetData(hstmt, i, SQL_C_BINARY, buffer.data(),
                                       (SQLLEN)buffer.size(), &indicator);

            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
              if (indicator == SQL_NULL_DATA) {
                row.Set(colName, std::nullopt);
              } else {
                size_t size = (indicator > 0 &&
                               indicator < static_cast<SQLLEN>(buffer.size()))
                                  ? static_cast<size_t>(indicator)
                                  : buffer.size();
                buffer.resize(size);
                row.Set(colName, buffer);
              }
            }
          } else if (nativeType == SQL_BIT) {
            char val;
            SQLRETURN ret =
                SQLGetData(hstmt, i, SQL_C_BIT, &val, sizeof(val), &indicator);

            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
              if (indicator == SQL_NULL_DATA) {
                row.Set(colName, std::nullopt);
              } else {
                bool boolVal = (val != 0);
                row.Set(colName, boolVal);
              }
            }
          } else if (nativeType == SQL_INTEGER || nativeType == SQL_SMALLINT ||
                     nativeType == SQL_TINYINT) {
            int valInt = 0;
            SQLRETURN ret =
                SQLGetData(hstmt, i, SQL_C_SLONG, &valInt, 0, &indicator);

            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
              if (indicator == SQL_NULL_DATA) {
                row.Set(colName, std::nullopt);
              } else {
                row.Set(colName, valInt);
              }
            }
          } else if (nativeType == SQL_DECIMAL || nativeType == SQL_NUMERIC ||
                     nativeType == SQL_REAL || nativeType == SQL_FLOAT ||
                     nativeType == SQL_DOUBLE) {
            double valDouble = 0.0;
            SQLRETURN ret =
                SQLGetData(hstmt, i, SQL_C_DOUBLE, &valDouble, 0, &indicator);

            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
              if (indicator == SQL_NULL_DATA) {
                row.Set(colName, std::nullopt);
              } else {
                row.Set(colName, valDouble);
              }
            }
          } else {
            char buffer[1024] = {0};
            SQLRETURN ret = SQLGetData(hstmt, i, SQL_C_CHAR, buffer,
                                       sizeof(buffer), &indicator);

            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
              if (indicator == SQL_NULL_DATA) {
                row.Set(colName, std::nullopt);
              } else {
                std::string valStr(buffer);
                row.Set(colName, valStr);
              }
            }
          }
        } catch (...) {
          row.Set(colName, std::nullopt);
        }
      }

      rows.push_back(std::move(row));
    }

    dataTable.Fill(rows);

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    hstmt = SQL_NULL_HSTMT;

    return dataTable;
  } catch (const std::exception &ex) {
    throw std::runtime_error(std::string("[SQLServerDatabase::FetchResults] Excepción: ") +
                             ex.what());
  }
}

omnisphere::types::DataTable
SQLServerDatabase::FetchPrepared(const std::string &query,
                                 const std::vector<std::string> &params) {
  omnisphere::types::DataTable dataTable;

  try {
    PrepareStatement(query);

    for (size_t i = 0; i < params.size(); ++i) {
      SQLRETURN retcode = SQLBindParameter(
          hstmt, static_cast<SQLUSMALLINT>(i + 1), SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_VARCHAR, 0, 0, (SQLPOINTER)params[i].c_str(), 0, nullptr);

      if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
        throw std::runtime_error("Error binding parameter " +
                                 std::to_string(i + 1));
    }

    SQLRETURN retcode = SQLExecute(hstmt);

    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
      throw std::runtime_error(
          ExtractError("SQLExecute", hstmt, SQL_HANDLE_STMT));

    SQLSMALLINT columnCount;
    SQLNumResultCols(hstmt, &columnCount);

    std::vector<std::string> columnNames(columnCount);
    std::vector<SQLSMALLINT> nativeTypes(columnCount);

    SQLCHAR columnName[256];
    SQLSMALLINT columnNameLength, decimalDigits, nullable;
    SQLULEN columnSize;

    for (SQLUSMALLINT i = 1; i <= columnCount; ++i) {
      SQLDescribeCol(hstmt, i, columnName, sizeof(columnName),
                     &columnNameLength, &nativeTypes[i - 1], &columnSize,
                     &decimalDigits, &nullable);
      columnNames[i - 1] =
          std::string(reinterpret_cast<char *>(columnName), columnNameLength);
    }

    std::vector<omnisphere::types::DataTable::Row> rows;

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
      omnisphere::types::DataTable::Row row;

      for (SQLUSMALLINT i = 1; i <= columnCount; ++i) {
        SQLLEN indicator = 0;
        SQLSMALLINT nativeType = nativeTypes[i - 1];
        const std::string &colName = columnNames[i - 1];

        if (nativeType == SQL_VARBINARY || nativeType == SQL_BINARY) {
          std::vector<uint8_t> buffer(512);
          SQLRETURN ret = SQLGetData(hstmt, i, SQL_C_BINARY, buffer.data(),
                                     (SQLLEN)buffer.size(), &indicator);

          if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            if (indicator == SQL_NULL_DATA) {
              row.Set(colName, std::nullopt);
            } else {
              size_t size = (indicator > 0 &&
                             indicator < static_cast<SQLLEN>(buffer.size()))
                                ? static_cast<size_t>(indicator)
                                : buffer.size();
              buffer.resize(size);
              row.Set(colName, buffer);
            }
          } else {
            row.Set(colName, std::nullopt);
          }
        } else if (nativeType == SQL_INTEGER || nativeType == SQL_BIGINT ||
                   nativeType == SQL_SMALLINT || nativeType == SQL_TINYINT) {
          int valInt = 0;
          SQLRETURN ret =
              SQLGetData(hstmt, i, SQL_C_SLONG, &valInt, 0, &indicator);

          if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            if (indicator == SQL_NULL_DATA) {
              row.Set(colName, std::nullopt);
            } else {
              row.Set(colName, valInt);
            }
          } else {
            row.Set(colName, std::nullopt);
          }
        } else if (nativeType == SQL_DOUBLE || nativeType == SQL_FLOAT ||
                   nativeType == SQL_REAL || nativeType == SQL_DECIMAL ||
                   nativeType == SQL_NUMERIC) {
          double valDouble = 0.0;
          SQLRETURN ret =
              SQLGetData(hstmt, i, SQL_C_DOUBLE, &valDouble, 0, &indicator);

          if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            if (indicator == SQL_NULL_DATA) {
              row.Set(colName, std::nullopt);
            } else {
              row.Set(colName, valDouble);
            }
          } else {
            row.Set(colName, std::nullopt);
          }
        } else {
          char buffer[512] = {0};
          SQLRETURN ret = SQLGetData(hstmt, i, SQL_C_CHAR, buffer,
                                     sizeof(buffer), &indicator);

          if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            if (indicator == SQL_NULL_DATA) {
              row.Set(colName, std::nullopt);
            } else {
              row.Set(colName, std::string(buffer));
            }
          } else {
            row.Set(colName, std::nullopt);
          }
        }
      }

      rows.push_back(std::move(row));
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    hstmt = nullptr;

    dataTable.Fill(rows);

    return dataTable;
  } catch (const std::exception &ex) {
    throw std::runtime_error(std::string("[SQLServerDatabase::FetchPrepared] Excepción: ") +
                             ex.what());
  }
}

omnisphere::types::DataTable SQLServerDatabase::FetchPrepared(const std::string &query,
                                                             const std::string &param) {
  return FetchPrepared(query, std::vector<std::string>{param});
}

omnisphere::types::DataTable SQLServerDatabase::FetchPrepared(
    const std::string &query,
    const std::vector<omnisphere::types::SQLParam> &params) {
  omnisphere::types::DataTable dataTable;

  try {
    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (!SQL_SUCCEEDED(ret))
      throw std::runtime_error(
          ExtractError("SQLAllocHandle", hdbc, SQL_HANDLE_DBC));

    SQLRETURN retcode = SQLPrepare(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(retcode))
      throw std::runtime_error(
          ExtractError("SQLPrepare", hstmt, SQL_HANDLE_STMT));

    stringStorage.clear();
    intStorage.clear();
    binaryStorage.clear();
    indStorage.clear();
    indStorage.resize(params.size());

    SQLUSMALLINT paramIndex = 1;

    struct ParamVisitor {
      SQLHSTMT hstmt;
      SQLUSMALLINT paramIndex;
      std::vector<SQLLEN> &indStorage;
      std::vector<int> &intStorage;
      std::vector<double> &doubleStorage;
      std::vector<std::string> &stringStorage;
      std::vector<std::vector<uint8_t>> &binaryStorage;

      SQLRETURN operator()(const std::monostate &) const {
        static char dummy = 0;
        indStorage[paramIndex - 1] = SQL_NULL_DATA;
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, 0, 0, &dummy, 0,
                                &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const int &value) const {
        intStorage.push_back(value);
        indStorage[paramIndex - 1] = sizeof(int);
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_SLONG,
                                SQL_INTEGER, 0, 0, &intStorage.back(), 0,
                                &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const double &value) const {
        doubleStorage.push_back(value);
        indStorage[paramIndex - 1] = sizeof(double);
        return SQLBindParameter(
            hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0,
            &doubleStorage.back(), 0, &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const std::string &value) const {
        stringStorage.emplace_back(value);
        std::string &storedStr = stringStorage.back();
        indStorage[paramIndex - 1] = static_cast<SQLLEN>(storedStr.size());
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, indStorage[paramIndex - 1], 0,
                                (SQLPOINTER)storedStr.c_str(), 0,
                                &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const std::vector<uint8_t> &value) const {
        binaryStorage.push_back(value);
        std::vector<uint8_t> &storedBin = binaryStorage.back();
        indStorage[paramIndex - 1] = static_cast<SQLLEN>(storedBin.size());
        return SQLBindParameter(
            hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY,
            indStorage[paramIndex - 1], 0, (SQLPOINTER)storedBin.data(),
            indStorage[paramIndex - 1], &indStorage[paramIndex - 1]);
      }

      SQLRETURN operator()(const bool &value) const {
        std::string ynValue = value ? "Y" : "N";
        stringStorage.emplace_back(ynValue);
        std::string &storedStr = stringStorage.back();
        indStorage[paramIndex - 1] = static_cast<SQLLEN>(storedStr.size());
        return SQLBindParameter(hstmt, paramIndex, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, indStorage[paramIndex - 1], 0,
                                (SQLPOINTER)storedStr.c_str(), 0,
                                &indStorage[paramIndex - 1]);
      }
    };

    for (const omnisphere::types::SQLParam &param : params) {
      ParamVisitor visitor{hstmt,        paramIndex,    indStorage,
                           intStorage,   doubleStorage, stringStorage,
                           binaryStorage};
      retcode = std::visit(visitor, param);

      if (!SQL_SUCCEEDED(retcode))
        throw std::runtime_error(
            ExtractError("SQLBindParameter", hstmt, SQL_HANDLE_STMT));

      ++paramIndex;
    }

    retcode = SQLExecute(hstmt);
    if (!SQL_SUCCEEDED(retcode))
      throw std::runtime_error(
          ExtractError("SQLExecute", hstmt, SQL_HANDLE_STMT));

    SQLSMALLINT columnCount;
    SQLNumResultCols(hstmt, &columnCount);

    std::vector<std::string> columnNames(columnCount);
    std::vector<SQLSMALLINT> nativeTypes(columnCount);

    SQLCHAR columnName[256];
    SQLSMALLINT columnNameLength, decimalDigits, nullable;
    SQLULEN columnSize;

    for (SQLUSMALLINT i = 1; i <= columnCount; ++i) {
      SQLDescribeCol(hstmt, i, columnName, sizeof(columnName),
                     &columnNameLength, &nativeTypes[i - 1], &columnSize,
                     &decimalDigits, &nullable);
      columnNames[i - 1] =
          std::string(reinterpret_cast<char *>(columnName), columnNameLength);
    }

    std::vector<omnisphere::types::DataTable::Row> rows;

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
      omnisphere::types::DataTable::Row row;

      for (SQLUSMALLINT i = 1; i <= columnCount; ++i) {
        SQLLEN indicator = 0;
        SQLSMALLINT nativeType = nativeTypes[i - 1];
        if (nativeType == SQL_VARBINARY || nativeType == SQL_BINARY) {
          std::vector<uint8_t> buffer(512);
          SQLRETURN ret = SQLGetData(hstmt, i, SQL_C_BINARY, buffer.data(),
                                     (SQLLEN)buffer.size(), &indicator);
          if (SQL_SUCCEEDED(ret)) {
            if (indicator == SQL_NULL_DATA)
              row.Set(columnNames[i - 1], std::nullopt);
            else {
              buffer.resize(indicator);
              row.Set(columnNames[i - 1], buffer);
            }
          }
        } else if (nativeType == SQL_INTEGER || nativeType == SQL_BIGINT) {
          int valInt = 0;
          SQLRETURN ret =
              SQLGetData(hstmt, i, SQL_C_SLONG, &valInt, 0, &indicator);
          if (SQL_SUCCEEDED(ret)) {
            if (indicator == SQL_NULL_DATA)
              row.Set(columnNames[i - 1], std::nullopt);
            else
              row.Set(columnNames[i - 1], valInt);
          }
        } else {
          char buffer[512];
          SQLRETURN ret = SQLGetData(hstmt, i, SQL_C_CHAR, buffer,
                                     sizeof(buffer), &indicator);
          if (SQL_SUCCEEDED(ret)) {
            if (indicator == SQL_NULL_DATA)
              row.Set(columnNames[i - 1], std::nullopt);
            else
              row.Set(columnNames[i - 1], std::string(buffer));
          }
        }
      }

      rows.push_back(std::move(row));
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    hstmt = SQL_NULL_HSTMT;

    dataTable.Fill(rows);
    return dataTable;
  } catch (const std::exception &ex) {
    if (hstmt != SQL_NULL_HSTMT) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      hstmt = SQL_NULL_HSTMT;
    }

    throw std::runtime_error(std::string("[SQLServerDatabase::FetchPrepared Exception] ") +
                             ex.what());
  }
}

} // namespace omnisphere::services
