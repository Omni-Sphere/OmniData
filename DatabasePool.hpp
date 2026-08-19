#pragma once

#include "Database.hpp"
#include "IDatabase.hpp"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace omnisphere::data
{
    /// Thread-safe connection pool with RAII-managed handles.
    /// Each Handle borrows one IDatabase connection for the duration of its scope.
    class DatabasePool
    {
    public:
        struct Config
        {
            ConnectionConfig connection;
            size_t           minConnections = 2;
            size_t           maxConnections = 10;
            std::chrono::seconds acquireTimeout{5};
            std::chrono::seconds idleTimeout{300}; // reconnect if idle longer than this
        };

        /// RAII handle: returned by Acquire(). Releases connection on destruction.
        class Handle
        {
        public:
            Handle(Handle&& other) noexcept;
            Handle& operator=(Handle&& other) noexcept;
            ~Handle();

            Handle(const Handle&)            = delete;
            Handle& operator=(const Handle&) = delete;

            /// Access the borrowed database connection.
            IDatabase& db();
            IDatabase* operator->();
            explicit operator bool() const noexcept { return _conn != nullptr; }
            bool operator!=(std::nullptr_t) const noexcept { return _conn != nullptr; }

        private:
            friend class DatabasePool;
            Handle(IDatabase* conn, DatabasePool* pool) noexcept;

            IDatabase*    _conn = nullptr;
            DatabasePool* _pool = nullptr;
        };

        explicit DatabasePool(const Config& cfg);
        ~DatabasePool();

        /// Block until a connection is available (up to acquireTimeout).
        Handle Acquire();

        /// Current number of idle connections in the pool.
        size_t Available() const;

        /// Total connections owned (idle + in use).
        size_t Size() const;

    private:
        void Release(IDatabase* conn);
        IDatabase* CreateAndConnect();

        Config _cfg;

        mutable std::mutex      _mx;
        std::condition_variable _cv;

        /// Owns all connections (idle and in-use alike).
        std::vector<std::unique_ptr<IDatabase>> _allConns;

        /// Pointers to idle connections (raw — ownership stays in _allConns).
        std::vector<IDatabase*> _idle;
    };

} // namespace omnisphere::data
