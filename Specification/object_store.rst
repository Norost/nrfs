Object store
============

Features
--------

* Error detection
* Error correction (with mirrors only!)
* Compression
* Transactional updates
* Pooling with mirroring & chaining (RAID 1+0)
* Defragmentation
* Sparse objects

Transactions
------------

To ensure consistency, all operations to the backing store are atomic.
This is achieved with transactional updates.

Implementing transactions requires that current data is *never* modified in place.
Instead, new space is allocated and modified data is written to that space.
At the end of each transaction the header is updated to point to the new data,
which is an atomic operation.

Allocation
----------

The allocation log keeps track of free space on the disk.
Each entry indicates an allocation or deallocation. [#alloc_log]_

When allocating or freeing space a new entry is appended.
The log is occasionaly rewritten to reduce space usage.

When loading the object store this log is replayed to initialize the allocator.

.. [#alloc_log] An allocation log is compact and gives great freedom in the
   type of allocator to be used.
   It can also be used as "append-only" to improve the performance of frequent
   allocations.

Storage
-------

All data is stored as records.
A record is a pointer to data with a hash and optional compression.
Records have a maximum size specified by the header.

If data does not fit in a single record a record tree is made.
In a record tree, all but the last record have the maximum size.

To help protect against corruption that may occur during transmission, bad
firmware or any other source a XXH3 hash is added to all records.

Mirroring
~~~~~~~~~

The filesystem can be mirrored to any number of disks.
This allows restoring corrupted data.

Chaining
~~~~~~~~

Multiple disks can be used for a single filesystem, increasing the capacity of
that filesystem.


Data Structures
---------------

All integers are in little-endian format.

Header
~~~~~~

A header is placed at the start and end of a volume.
A header has a variable size, up to 64 KiB.

.. table:: FS Header
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+            Magic string ("Nora Reliable FS")          |
  |    8 |                                                       |
  +------+------+------+------+------+------+------+-------------+
  |   16 | MirI | MirC | RLen | BLen | CAlg | Ver  |             |
  +------+------+------+------+------+------+------+-------------+
  |   24 |                                                       |
  +------+                          UID                          |
  |   32 |                                                       |
  +------+-------------------------------------------------------+
  |   32 |                   Total block count                   |
  +------+-------------------------------------------------------+
  |   40 |                      LBA offset                       |
  +------+-------------------------------------------------------+
  |   48 |                      Block count                      |
  +------+-------------------------------------------------------+
  |   64 |                                                       |
  +------+                                                       |
  |   72 |                                                       |
  +------+                      Object list                      |
  |   80 |                                                       |
  +------+                                                       |
  |   88 |                                                       |
  +------+-------------------------------------------------------+
  |   96 |                                                       |
  +------+                                                       |
  |  104 |                                                       |
  +------+                     Allocation log                    |
  |  112 |                                                       |
  +------+                                                       |
  |  120 |                                                       |
  +------+-------------------------------------------------------+
  |  128 |                         XXH3                          |
  +------+-------------------------------------------------------+
  |  136 |                      Generation                       |
  +------+-------------------------------------------------------+
  |  144 |                                                       |
  +------+                                                       |
  |  ... |                       Reserved                        |
  +------+                                                       |
  |  248 |                                                       |
  +------+-------------------------------------------------------+
  |  256 |                                                       |
  +------+                                                       |
  |  ... |              Free for use by filesystem               |
  +------+                                                       |
  |  504 |                                                       |
  +------+-------------------------------------------------------+

* Magic string: Must always be "Nora reliable FS"

* Vers: The version of the data storage format.
  Must have the value 2 as of writing.

* CAlg: The default compression algorithm to use.

* BLen: The length of a single block as a power of two.
  Affects LBA addressing.

* RLen: The maximum length of a record in bytes as a power of two.

* MirC: The amount of mirror volumes.
  Useful to determine how many mirrors should be waited for before allowing
  writes.

* MirI: The index of this chain in the mirror list.
  It simplifies loading code & prevents devices from being shuffled between
  chains on each mount.

* UID: Unique filesystem identifier.

* Total block count:
  The total amount of blocks this pool consists of.

* LBA offset: The offset to add to all LBAs on this disk.

* Block count: The amount of blocks in this pool.

* Object list: Record tree containing a list of objects.

* Allocation log LBA: The start block of the allocation log.
  There is one log per pool.

* Allocation log length: The length of the allocation log in bytes.

* XXH3: Hash of the header.
  This field is zeroed before hashing.

* Generation: Counts updates. Wraps arounds.

* Reserved: unused space that is set aside for any potential updates to this
  specification.
  **Must** be zeroed.

All bytes between 256 and 512 are free for use by the filesystem layer.

  When updating the headers, ensure the updates *do not* happen concurrently.


Record
~~~~~~

A record represents a single unit of data.

.. table:: Record
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                          LBA                          |
  +------+-------------+------+------+---------------------------+
  |    8 | References  |      | CAlg |        Data length        |
  +------+-------------+------+------+---------------------------+
  |   16 |                         XXH3                          |
  +------+-------------------------------------------------------+
  |   24 |                     Total length                      |
  +------+-------------------------------------------------------+

* LBA: The address of starting address of the data.

* Data length: The length of the *compressed* data.

* CAlg: The compression algorithm used on the data.

.. table:: Compression algorithms

  +----+------+
  | ID | Name |
  +====+======+
  |  0 | None |
  +----+------+
  |  1 | LZ4  |
  +----+------+

* References: The amount of pointers to this record.
  Only used by record trees.

* XXH3: XXH3 hash of the *compressed* data.
  Used to verify integrity.
  If the data length is 0, the XXH3 shall have a value of 0 [#]_.

.. [#] Zeroing the XXH3 is necessary to have effective zero-optimization.

* Total length: The total length of all data.
  Only used by record trees.


Record tree
~~~~~~~~~~~

A record tree respresents a group of data.
If a tree has a depth greater than 0 it consists of multiple subtrees.
These subtrees do *not* have a total length set.
The depth is derived from the total length and the maximum record size.

The depth of a record tree depends on the size of the data.

::
  
  x = ceil(max(1, len), max_rec_size) / max_rec_size
  depth = ceil(log(x, max_rec_size / 32), 1)

Some records may not unpack to the expected length.
The remaining length is all zeroes [#]_.

.. [#] This optimization is called "zero-optimization" and is essential for
   sparse objects.


Object list
~~~~~~~~~~~

The object list keeps track of record trees (except for itself).
Objects are indexed by ID.
If the reference count of an object is greater than zero, it is in use.
Otherwise it is free.
Determining which slots are free is done by scanning the entire list [#]_.

.. [#] This scanning can be done after the object store is mounted. If a new
   object must be allocated before the scanning is done, append it to the list.


Allocation log
~~~~~~~~~~~~~~

The allocation log keeps track of allocations and deallocations [#]_.

.. [#] An allocation log is much more convenient to use with transactional
   filesystems.
   It can also, combined with defragmentation, be much more compact than e.g.
   a bitmap as a single log entry can cover a very large range for a fixed
   cost.

   The log can be rewritten at any points to compactify it.

The log is kept track of as a linked list [#]_,
where the first 32 bytes are a record pointing to the next element and all
bytes after it are log entries.
The bottom of the stack denotes the start of the log.

.. [#] A linked stack has the following useful properties:

   * Appending is very quick.
     This makes transactions quicker if I/O load is high.
   * There are no parent records that need to be modified.

   Additionally, deriving the allocation status of any block can trivially be
   determined while iterating by "xor"ing the entries together.
   i.e. the status of a block is indicates by the amount of entries that
   refer to said block.

The space used by records for the stack are **not** explicitly recorded in the
log [#]_.

.. [#] This makes it practical to compress log records.

   The space used by these records can trivially be derived while iterating the
   stack.

.. table:: Log stack element

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                                                       |
  |    8 |                                                       |
  +------+                      Next record                      |
  |   16 |                                                       |
  +------+                                                       |
  |   24 |                                                       |
  +------+-------------------------------------------------------+
  |  ... |                                                       |
  +------+-------------------------------------------------------+

.. table:: Log entry

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                          LBA                          |
  +------+-------------------------------------------------------+
  |    8 |                          Size                         |
  +------+-------------------------------------------------------+

Each log entry inverts the status of the range covered (i.e. ``xor``).
Each log entry indicates either an allocation or deallocation,
never both partially.
The length of each entry may never be 0.
