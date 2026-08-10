#include "Database.hpp"
#include "SQLServerDatabase.hpp"
#include "MySQLDatabase.hpp"
#include "PostgreSQLDatabase.hpp"

namespace omnisphere::data
{
    Database::Database(DatabaseEngine engine)
    {
        if (engine == DatabaseEngine::MySQL)
        {
            _impl = std::make_unique<MySQLDatabase>();
        }
        else if (engine == DatabaseEngine::PostgreSQL)
        {
            _impl = std::make_unique<PostgreSQLDatabase>();
        }
        else
        {
            _impl = std::make_unique<SQLServerDatabase>();
        }
    }

    Database::Database(const ConnectionConfig& cfg)
        : Database(cfg.dbEngine)
    {
        std::string connStr = BuildConnectionString(cfg);
        _impl->ConnectionString(connStr);
    }

    Database::~Database() = default;
    // Sobrecarga que recibe el DTO completo
    std::string Database::BuildConnectionString(const ConnectionConfig& cfg)
    {
        return BuildConnectionString(
            cfg.dbEngine,
            cfg.server,
            cfg.database,
            cfg.user,
            cfg.password,
            cfg.trustCertificate,
            cfg.trustedConnection
        );
    }

    std::string Database::BuildConnectionString(
        DatabaseEngine engine,
        const std::string& server,
        const std::string& database,
        const std::string& user,
        const std::string& password,
        bool trustCert,
        bool trustedConn)
    {
        std::string connStr;
        switch (engine)
        {
            case DatabaseEngine::SQLServer:
                connStr = "Driver={ODBC Driver 18 for SQL Server};Server=" + server + ";";
                if (!database.empty()) connStr += "Database=" + database + ";";
                if (trustedConn) {
                    connStr += "Trusted_Connection=yes;";
                } else {
                    connStr += "Uid=" + user + ";Pwd=" + password + ";";
                }
                connStr += "TrustServerCertificate=" + std::string(trustCert ? "yes" : "no") + ";";
                break;

            case DatabaseEngine::MySQL:
                connStr = "Driver={MySQL ODBC 9.4 Driver};Server=" + server + ";";
                if (!database.empty()) connStr += "Database=" + database + ";";
                connStr += "User=" + user + ";Password=" + password + ";";
                break;

            case DatabaseEngine::PostgreSQL:
            {
                std::string host = server;
                std::string port = "5432";
                size_t colonPos = server.find(':');
                if (colonPos != std::string::npos)
                {
                    host = server.substr(0, colonPos);
                    port = server.substr(colonPos + 1);
                }
                connStr = "Driver={PostgreSQL Unicode};Server=" + host + ";Port=" + port + ";";
                if (!database.empty()) connStr += "Database=" + database + ";";
                connStr += "Uid=" + user + ";Pwd=" + password + ";";
                break;
            }
        }
        return connStr;
    }   

} // namespace omnisphere::data