# Sql Implementations, one for all projects.

## Why?

- As a Backend Engineer, my daily work mostly involved CRUD operations with databases, APIs, architectures, third-party integrations, and additional business logic. The thing is, before going further, deep diving into a DB is a good point, but I realized I lacked many fundamental aspects of engineering.
- DBMS is a challenging thing to implement because it involves all the complex aspects of programming: from fundamentals like operating systems, networking, and parallel/concurrent programming, to more advanced topics like distributed systems, caching, scaling, and various complex data structures that need to be understood.
- I want to make a change and truly understand what happens behind the scenes, so I decided to create a database one from scratch.

## Lesson learned

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
