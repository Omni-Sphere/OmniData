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

} // namespace omnisphere::data