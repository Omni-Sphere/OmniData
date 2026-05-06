#include <Database.hpp>
#include <SQLServerDatabase.hpp>
#include <MySQLDatabase.hpp>

namespace omnisphere::services {

Database::Database(DatabaseEngine engine) {
  if (engine == DatabaseEngine::MySQL) {
    _impl = std::make_unique<MySQLDatabase>();
  } else {
    _impl = std::make_unique<SQLServerDatabase>();
  }
}

} // namespace omnisphere::services