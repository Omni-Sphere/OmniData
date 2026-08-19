#include "DatabasePool.hpp"

#include <stdexcept>
#include <string>

namespace omnisphere::data
{
    // ─── Handle ───────────────────────────────────────────────────────────────

    DatabasePool::Handle::Handle(IDatabase* conn, DatabasePool* pool) noexcept
        : _conn(conn), _pool(pool)
    {}

    DatabasePool::Handle::Handle(Handle&& other) noexcept
        : _conn(other._conn), _pool(other._pool)
    {
        other._conn = nullptr;
        other._pool = nullptr;
    }

    DatabasePool::Handle& DatabasePool::Handle::operator=(Handle&& other) noexcept
    {
        if (this != &other)
        {
            // Release current connection back to pool
            if (_conn && _pool)
                _pool->Release(_conn);

            _conn       = other._conn;
            _pool       = other._pool;
            other._conn = nullptr;
            other._pool = nullptr;
        }
        return *this;
    }

    DatabasePool::Handle::~Handle()
    {
        if (_conn && _pool)
            _pool->Release(_conn);
    }

    IDatabase& DatabasePool::Handle::db()
    {
        if (!_conn)
            throw std::runtime_error("[DatabasePool::Handle] Connection already released.");
        return *_conn;
    }

    IDatabase* DatabasePool::Handle::operator->()
    {
        return &db();
    }

    // ─── DatabasePool ─────────────────────────────────────────────────────────

    DatabasePool::DatabasePool(const Config& cfg) : _cfg(cfg)
    {
        if (_cfg.minConnections > _cfg.maxConnections)
            throw std::invalid_argument(
                "[DatabasePool] minConnections > maxConnections.");
        if (_cfg.maxConnections == 0)
            throw std::invalid_argument(
                "[DatabasePool] maxConnections must be > 0.");

        // Pre-warm minimum connections
        std::lock_guard<std::mutex> lock(_mx);
        for (size_t i = 0; i < _cfg.minConnections; ++i)
        {
            IDatabase* raw = CreateAndConnect();
            _idle.push_back(raw);
        }
    }

    DatabasePool::~DatabasePool()
    {
        std::lock_guard<std::mutex> lock(_mx);
        // All connections (idle + in-use) are owned by _allConns;
        // unique_ptr destructors call Disconnect() via ~Database().
        _idle.clear();
        _allConns.clear();
    }

    // ─── Acquire ──────────────────────────────────────────────────────────────

    DatabasePool::Handle DatabasePool::Acquire()
    {
        std::unique_lock<std::mutex> lock(_mx);

        auto deadline = std::chrono::steady_clock::now() + _cfg.acquireTimeout;

        // Wait until either an idle connection is available
        // or we can create a new one within the max limit.
        while (_idle.empty() && _allConns.size() >= _cfg.maxConnections)
        {
            if (_cv.wait_until(lock, deadline) == std::cv_status::timeout)
                throw std::runtime_error(
                    "[DatabasePool::Acquire] Timed out waiting for an available connection.");
        }

        IDatabase* raw = nullptr;

        if (!_idle.empty())
        {
            raw = _idle.back();
            _idle.pop_back();
        }
        else
        {
            // Create a new connection under the max limit
            raw = CreateAndConnect();
        }

        return Handle(raw, this);
    }

    // ─── Release ──────────────────────────────────────────────────────────────

    void DatabasePool::Release(IDatabase* conn)
    {
        {
            std::lock_guard<std::mutex> lock(_mx);
            _idle.push_back(conn);
        }
        _cv.notify_one();
    }

    // ─── CreateAndConnect ─────────────────────────────────────────────────────

    IDatabase* DatabasePool::CreateAndConnect()
    {
        // Build connection string and create a Database instance
        std::string connStr = Database::BuildConnectionString(_cfg.connection);
        auto        db      = std::make_unique<Database>(_cfg.connection.dbEngine);
        db->ConnectionString(connStr);
        db->Connect();

        IDatabase* raw = db.get();
        _allConns.push_back(std::move(db));
        return raw;
    }

    // ─── Status helpers ───────────────────────────────────────────────────────

    size_t DatabasePool::Available() const
    {
        std::lock_guard<std::mutex> lock(_mx);
        return _idle.size();
    }

    size_t DatabasePool::Size() const
    {
        std::lock_guard<std::mutex> lock(_mx);
        return _allConns.size();
    }

} // namespace omnisphere::data
