Object store
============

Features
--------

* Error detection
* Error correction (with mirrors only!)
* Compression
* Transactional updates
* Mirroring
* Defragmentation
* Sparse objects

Transactions
------------

To ensure consistency, all operations to the backing store are atomic.
That is, current data is *never* modified in-place.
Instead, new space is allocated and modified data is written to that space.
At the end of each transaction the header is updated to point to the new data,
which is an atomic operation.

Allocation
----------

Free & allocated space is tracked in a log.
When loading the filesystem this log is replayed to initialize the allocator.
When allocating or freeing space a new entry is appended.
The log is occasionaly rewritten to reduce space usage.
By default, all space is assumed to be free.

Storage
-------

All data is stored as records.
A record is a pointer to data with a hash and optional compression.
Records have a maximum size specifiedby the header.
If data does not fit in a single record a record tree is made.
In a record tree, all but the last record have the maximum size.

Deduplication & defragmentation
-------------------------------

When checking if two files are identical, the hashes of their root records are
compared.
Deduplication is only done at the *file* level.
This is to ensure that files can be defragmented at any time.
To defragment a file a new allocation is made and the file data is copied to
this allocation.

Data Structures
---------------

All integers are in little-endian format.

Header
~~~~~~

.. table:: FS Header

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+            Magic string ("Nora Reliable FS")          |
  |    8 |                                                       |
  +------+------+------+------+------+---------------------------+
  |   16 | BLen | RLen | CAlg | HAlg |   Version (0x00_00_0002)  |
  +------+------+------+------+------+---------------------------+
  |   24 |                   Allocation log LBA                  |
  +------+-------------------------------------------------------+
  |   32 |                  Allocation log length                |
  +------+-------------------------------------------------------+
  |   40 |                                                       |
  +------+                                                       |
  |   48 |                                                       |
  +------+                                                       |
  |   56 |                                                       |
  +------+-------------------------------------------------------+
  |   64 |                                                       |
  +------+                                                       |
  |   72 |                                                       |
  +------+                                                       |
  |   80 |                                                       |
  +------+                                                       |
  |   88 |                                                       |
  +------+               Object List (record tree)               |
  |   96 |                                                       |
  +------+                                                       |
  |  104 |                                                       |
  +------+                                                       |
  |  112 |                                                       |
  +------+                                                       |
  |  120 |                                                       |
  +------+-------------------------------------------------------+


Record
~~~~~~

.. table:: Record

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                                                       |
  |    8 |                                                       |
  +------+                         Hash                          |
  |   16 |                                                       |
  +------+                                                       |
  |   24 |                                                       |
  +------+-------------------------------------------------------+
  |   32 |                          LBA                          |
  +------+-------------+------+------+---------------------------+
  |   40 | References  | CAlg | HAlg |          Length           |
  +------+-------------+------+------+---------------------------+
  |   48 |                     Total length²                     |
  +------+-------------------------------------------------------+
  |   56 |                                                       |
  +------+-------------------------------------------------------+

.. table:: Hash algorithms

  +----+------+
  | ID | Name |
  +====+======+
  |  0 | None |
  +----+------+

.. table:: Compression algorithms

  +----+------+
  | ID | Name |
  +====+======+
  |  0 | None |
  +----+------+
  |  1 | LZ4  |
  +----+------+

² Only used by record trees.


Record tree
~~~~~~~~~~~

A record tree respresents a group of data.
If a tree has a depth greater than 0 it consists of multiple subtrees.
These subtrees do *not* have a total length set.
The depth is derived from the total length and the maximum record size.

The type should be set to 0 to ensure deduplication & recovery is effective.

The depth of a record tree depends on the size of the data.
``ceil(log2(length / size of record))``

Some records may not unpack to the expected length.
The remaining length is all zeroes.

Object list
~~~~~~~~~~~

The object list keeps track of record trees (except for itself).


Log
~~~

.. table:: Log entry

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                          LBA                          |
  +------+-------------------------------------------------------+
  |    8 |                          Size                         |
  +------+-------------------------------------------------------+

If the high bit of Size is set the entry is a deallocation.
Otherwise it is an allocation.
