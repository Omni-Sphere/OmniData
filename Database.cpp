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
        // Helper: extract host and port from "host:port" or plain "host"
        auto splitHostPort = [](const std::string& s, const std::string& defaultPort)
            -> std::pair<std::string, std::string>
        {
            size_t pos = s.find(':');
            if (pos != std::string::npos)
                return {s.substr(0, pos), s.substr(pos + 1)};
            return {s, defaultPort};
        };

        std::string connStr;

        switch (engine)
        {
            // ── SQL Server: ODBC (unchanged) ───────────────────────────────
            case DatabaseEngine::SQLServer:
            {
                connStr = "Driver={ODBC Driver 18 for SQL Server};Server=" + server + ";";
                if (!database.empty()) connStr += "Database=" + database + ";";
                if (trustedConn)
                    connStr += "Trusted_Connection=yes;";
                else
                    connStr += "Uid=" + user + ";Pwd=" + password + ";";
                connStr += "TrustServerCertificate=";
                connStr += (trustCert ? "yes" : "no");
                connStr += ";";
                break;
            }

            // ── PostgreSQL: libpq key=value format ─────────────────────────
            // Accepted directly by PQconnectdb().
            case DatabaseEngine::PostgreSQL:
            {
                auto [host, port] = splitHostPort(server, "5432");
                connStr  = "host=" + host;
                connStr += " port=" + port;
                if (!database.empty()) connStr += " dbname=" + database;
                connStr += " user=" + user;
                connStr += " password=" + password;
                // Optional: connect_timeout, sslmode, etc. can be appended here
                break;
            }

            // ── MySQL: semicolon key=value format ──────────────────────────
            // Parsed by MySQLDatabase::ParseConnectionString().
            case DatabaseEngine::MySQL:
            {
                auto [host, port] = splitHostPort(server, "3306");
                connStr  = "host=" + host + ";";
                connStr += "port=" + port + ";";
                if (!database.empty()) connStr += "dbname=" + database + ";";
                connStr += "user=" + user + ";";
                connStr += "password=" + password + ";";
                break;
            }
        }
        return connStr;
    }

} // namespace omnisphere::data