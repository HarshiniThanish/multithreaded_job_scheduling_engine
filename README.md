# Multithreaded Job Scheduling Engine

A compact C++20 job scheduler demo with a priority queue, fixed-size worker pool,
retry policy, SQLite persistence, a small REST-like HTTP API, and a browser dashboard.

## Requirements
- CMake 3.20+
- C++20 compiler (GCC 11+, Clang 14+, or recent MSVC)
- SQLite development headers/library


## API
- `GET /health`
- `GET /stats`
- `GET /jobs`
- `POST /jobs` JSON: `{"name":"demo","priority":5,"failures":1}`
- `POST /shutdown`
