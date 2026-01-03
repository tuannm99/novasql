# [01] NovaSQL

NovaSQL is a small database engine written in Go, built as a learning project to understand how databases work under the hood.

> Note: NovaSQL was built quickly as a learning prototype (70% were AI-assisted). The codebase has become harder to maintain than I’d like.
> I’m stopping development here and starting a new project from scratch — fully hand-written, with a simpler architecture and stricter standards.

---

## Run

```sh
# 1) Start server
go run ./cmd/server -config novasql.yaml
# novasql tcp server listening on 127.0.0.1:8866 (workdir=./data_test)

# 2) Start client (CLI)
go run ./cmd/client -addr 127.0.0.1:8866

novasql> \help
# meta commands:
#   \q | quit | exit       quit
#   \history               print history
#   \help                  show help
#
# sql:
#   end statement with ';' (parser requires it)
#   multiline is supported (CLI will wait until ';')


> CREATE DATABASE testdb;
> USE testdb;
> CREATE TABLE users (id INT64 NOT NULL, name TEXT, active BOOL);
> INSERT INTO users VALUES (1, "a", true);
> INSERT INTO users VALUES (2, "b", true);

> SELECT * FROM users;
> UPDATE users SET name="bb" WHERE id=2;
> DELETE FROM users WHERE id=1;

> SELECT * FROM users;

```

---

## 🔍 High-Level Overview

NovaSQL focuses on:

- **Page-based storage** with slotted pages
- **Buffer pool** with a CLOCK replacement policy
- **Heap tables** on top of pages
- **Index structures** (B+Tree – in progress)
- **Write-Ahead Logging (WAL) & Transactions** (planned)
- **SQL front-end** (parser / planner / executor – planned)

The project is structured to mirror real-world database engines (SQLite/PostgreSQL) but stays small enough to remain hackable and educational.

---

## Features (Current)

### Storage Engine

- **Page-based storage** (fixed-size pages, slotted pages)
- **Segmented files** (`Base`, `Base.1`, `Base.2`, …)
- **Heap tables**
  - `INSERT`, `GET`, `SCAN`, `UPDATE`, `DELETE`
- **Overflow storage** for large rows (heap tuple points to overflow chain)
  - Best-effort free on `UPDATE` / `DELETE`

### Buffer Pool

- **Global shared buffer pool** (shared across heap/index/overflow)
- **CLOCK replacement policy**
- **Per-FileSet view** (`Database.BufferView(fs)`) for relation-scoped access

### Indexes (Early)

- **B+Tree index** (persisted pages)
- Supports:
  - `Insert(key, tid)`
  - `SearchEqual(key)` (duplicates supported)

### SQL Layer

- Minimal SQL pipeline:
  - Parser → Planner → Executor
- Basic plans:
  - `CREATE DATABASE`, `DROP DATABASE`, `USE`
  - `CREATE TABLE`, `DROP TABLE`
  - `INSERT`
  - `SELECT` (SeqScan)
  - `SELECT` via IndexLookup (when planner chooses it)
  - `UPDATE`
  - `DELETE`
- **Index maintenance (best-effort)**
  - INSERT: executor inserts into BTree
  - UPDATE/DELETE: may create stale index entries (executor re-checks heap row)

### TCP Server + CLI Client

- TCP server that speaks a simple framed protocol (`server/sqlwire`)
- Interactive CLI client:
  - multi-line SQL (ends with `;`)
  - `\help`, `\history`, `\q`

---

## Project Layout

```text
cmd/
  server/      TCP server entrypoint
  client/      CLI client entrypoint
internal/
  storage/     pages, segments, storage manager, overflow
  bufferpool/  global pool + CLOCK
  heap/        heap table
  btree/       B+Tree index
  sql/
    parser/
    planner/
    executor/
server/
  novasqlsqlwire/     frame protocol definitions + encode/decode
sqlclient/     reusable TCP client package (if you keep it here)
```

---

## 🧱 Architecture

At a high level:

```text
┌────────────────────────────────────────────┐
│                Client Layer                │
└───────────────────┬────────────────────────┘
                    │  (SQL text / driver)
┌───────────────────▼────────────────────────┐
│              Query Processor               │
│  ┌─────────────┐  ┌───────────┐  ┌───────┐ │
│  │   Parser    │──▶ Optimizer │──▶ Exec  │ │
│  └─────────────┘  └───────────┘  └───────┘ │
└───────────────────┬────────────────────────┘
                    │  (logical ops: scan, join, filter)
┌───────────────────▼────────────────────────┐
│              Storage Engine                │
│  ┌─────────────┐  ┌───────────┐  ┌───────┐ │
│  │   Tables    │  │  Indexes  │  │  WAL  │ │
│  └─────────────┘  └───────────┘  └───────┘ │
└───────────────────┬────────────────────────┘
                    │  (logical pages)
┌───────────────────▼────────────────────────┐
│            Buffer Pool + Pages             │
│  ┌─────────────┐  ┌───────────┐           │
│  │  BufferPool │  │   Pages   │           │
│  └─────────────┘  └───────────┘           │
└───────────────────┬────────────────────────┘
                    │  (physical pages)
┌───────────────────▼────────────────────────┐
│              Disk Management               │
└────────────────────────────────────────────┘

```

---

## Roadmap (Next)

### Planned exploration:

- **LSM-tree storage**
- **Ring / Cassandra-like partitioning**
- **Distributed KV store** (Consul-like)

### Challenges & Future Exploration

- **Advanced compression algorithms**
- **Distributed database concepts**
- **Streaming capabilities**
- **Column-store implementation**
- **Time-series optimizations**
- **Graph database extensions**

---

## 🙏 Acknowledgements

This project draws inspiration from various database systems and educational resources, including:

- SQLite
- PostgreSQL
- CMU Database Systems Course
- Various systems programming books and resources
