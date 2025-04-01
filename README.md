# NovaSQL

## 🔍 Overview

NovaSQL is a database system written from scratch as a learning journey into database internals. This project aims to implement core database concepts while gaining deeper understanding of low-level programming, memory management, and file systems.

## 🎯 Project Goals

- Build a working SQL database engine from first principles
- Understand database internals through implementation
- Learn systems programming fundamentals
- Document the journey and lessons learned

## 📚 Implementation Focus Areas

### Core Database Components

- **Page Management**
  - Slotted Page implementation
  - B+Tree indexing structure
  - Free Space Management (FSM)
  - Hash Index
  - Log-Structured Merge Tree (LSM-Tree)

- **SQL Processing**
  - Query parsing and optimization
  - Execution plans
  - Join implementations
  - ORM layer built from scratch

- **ACID Properties**
  - Write-Ahead Logging (WAL)
  - Transaction management
  - Isolation levels implementation
  - Lock management & latches

- **Operations**
  - Backup mechanisms
  - Triggers
  - Compression algorithms

### System-Level Fundamentals

- Binary data manipulation and byte processing
- Operating system interactions
- Disk I/O management
- Network protocol implementation
- Concurrency control

## 🧠 Lessons Learned

### Programming Insights

- **Low-Level Programming**
  - Binary & byte processing complexities
  - Operating system interactions
  - Network programming fundamentals

- **Data Structures & Algorithms**
  - Bloom filter implementation
  - B+Tree for efficient indexing
  - Clock algorithm
  - LRU cache mechanisms
  - Various sorting algorithms

- **Database Concepts**
  - Caching strategies
  - ORM implementation from first principles
  - ACID properties in practice
  - Concurrent programming patterns

### Project Management Insights

- Time management is crucial, especially allocating research time
- Breaking complex problems into manageable pieces is essential
- Avoid "unsafe" operations when possible
- Leverage third-party packages for improved concurrency
- Design patterns can be cumbersome in low-level programming
  - They take longer to implement
  - Abstraction can complicate code at lower levels
- Documentation is most effective when completed after implementation

## 🔬 Fundamental Concepts

### Data Persistence
- Working with in-memory data structures is challenging; persisting them to disk adds complexity
- Essential understanding of operating systems, byte manipulation, and file systems required

### Database Core Operations
- Database disk write patterns
- CPU interactions with disk and memory
- Client-server communication (SQL statement transmission)
- SQL statement parsing and execution planning

## 🛣️ Roadmap

### Current Implementation
- Basic page management
- Initial B+Tree structure
- Fundamental query processing

### Planned Features
- Enhanced ACID compliance
- More join algorithms
- Improved transaction isolation
- Backup & recovery systems
- Triggers
- Advanced compression techniques

## 🏗️ Architecture

```
┌────────────────────────────────────────────┐
│                Client Layer                │
└───────────────────┬────────────────────────┘
                    │
┌───────────────────▼────────────────────────┐
│              Network Protocol              │
└───────────────────┬────────────────────────┘
                    │
┌───────────────────▼────────────────────────┐
│              Query Processor               │
│  ┌─────────────┐  ┌───────────┐  ┌───────┐ │
│  │    Parser   │──▶ Optimizer │──▶ Exec  │ │
│  └─────────────┘  └───────────┘  └───────┘ │
└───────────────────┬────────────────────────┘
                    │
┌───────────────────▼────────────────────────┐
│            Storage Engine                  │
│  ┌─────────────┐  ┌───────────┐  ┌───────┐ │
│  │   B+Tree    │  │    WAL    │  │ Buffer│ │
│  └─────────────┘  └───────────┘  └───────┘ │
└───────────────────┬────────────────────────┘
                    │
┌───────────────────▼────────────────────────┐
│              Disk Management               │
└────────────────────────────────────────────┘
```

## 🤔 Challenges & Future Exploration

The journey of building NovaSQL has revealed many areas for future exploration:

- Advanced compression algorithms
- Distributed database concepts
- Streaming capabilities
- Column-store implementation
- Time-series optimizations
- Graph database extensions

## 🙏 Acknowledgements

This project draws inspiration from various database systems and educational resources, including:
- SQLite
- PostgreSQL
- CMU Database Systems Course
- Various systems programming books and resources
