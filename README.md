# Sql Implementations, one for all projects.

## Lesson learned

### programming

- Binary, bytes processing
- Operating system
- Basic networking
- Data structures: bloom filter, bplustree, clock, LRU cache, sorting algorithms
- Caching strategy
- ORM from scratch
- Database internal
- Deeper understand of ACID, how it implemented
- Concurrent programming: dealing with locks, race condition, data race, concurrent pattern (only go)

### others

- Time management is an important aspect to consider when starting a project, including research time and breaking big problems into smaller pieces.
- We don't want to mess with "unsafe".
- Using third-party packages for better concurrent processing.
- Design patterns can feel cumbersome in low-level programming; they often take more time to implement, and abstracting things can make the code harder to understand.
- Documenting after completing the work makes it easier to follow when coming back after a long time.

## Basic things to understand before doing

- Working with data structures in memory was pretty hard, but it becomes even harder when we persist them to disk. We really need to understand the operating system and be comfortable working with bytes, binary data, and the file system
- How Db write data on disk
- How CPU process/read/write data from disk, memory
- How client send a Statement to database (networking?)
- How Statement are transformed from SQL language to things that DB can understand and execute

## Page implementations fundamentals

- Slotted Page
- BTree
- FSM
- Hash Index
- LMSTree?

## Compression

-
-

## Join

-

## ACID implementations

- WAL?
- Transactions?
- Isolation Level implementations?
- Locks & Latches

## Backup/Trigger

## Things to do next

- No idea, before learning more
