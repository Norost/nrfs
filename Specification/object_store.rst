Object store
============

Features
--------

* Maximum volume size of `2^64 - 1` blocks

  * With a maximum block size of `2^24`, maximum volume size is `2^88 - 2^24`
    bytes

* Maximum object size of `2^64 - 1` bytes (depends on record size)
* Maximum object count of `2^58`
* Error detection
* Error correction (with mirrors only!)
* Compression
* Encryption
* Transactional updates
* Pooling with mirroring & chaining (RAID 1+0)
* Defragmentation
* Sparse objects


Partition identifier
--------------------

See the filesystem document for the partition identifier.


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
Each entry indicates an allocation or deallocation. [#]_

When allocating or freeing space a new entry is appended.
The log is occasionaly rewritten to reduce space usage.

When loading the object store this log is replayed to initialize the allocator.

.. [#] An allocation log is compact and gives great freedom in the
   type of allocator to be used.
   It can also be used as "append-only" to improve the performance of frequent
   allocations.

Storage
-------

All data is stored as records.
A record is a unit of packed data.
Packing involves compressing, encrypting, hashing and prefixing a header.

Records have a maximum size. This size is specified in the filesystem header.

If data does not fit in a single record a record tree is made.
In a record tree, all but the last record have the maximum size.

To help protect against corruption that may occur during transmission, bad
firmware or any other source a hash is added to all records.

Mirroring
~~~~~~~~~

The filesystem can be mirrored to up to 4 chains of disks.
This allows restoring corrupted data.

Chaining
~~~~~~~~

Multiple disks can be chained, increasing the capacity of the filesystem.


Data Structures
---------------

All integers are in little-endian format.

Filesystem info
~~~~~~~~~~~~~~~

A block with a filesystem header & info is placed at the start and end of a volume.

  It is sufficient to only read the start headers when loading a filesystem.
  Scanning for tail headers is useful if the start header is corrupted.

.. table:: Filesystem header

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 | KDF  | Ciph | BlkS | Ver. |       Magic string        |
  +------+------+------+------+------+---------------------------+
  |    8 |               Key derivation parameters               |
  +------+-----------------------------------------+-------------+
  |   16 |                                         |  Key hash   |
  +------+-----------------------------------------+-------------+
  |   24 |                         Nonce                         |
  +------+-------------------------------------------------------+
  |   32 |                                                       |
  +------+                          UID                          |
  |   40 |                                                       |
  +------+-------------------------------------------------------+
  |   48 |                                                       |
  +------+                         Hash                          |
  |   56 |                                                       |
  +------+-------------------------------------------------------+

* Magic string: Defined by the upper layer.
  See the filesystem document for the value.

* Ver.: The version of the data storage format.
  Must have the value 1 as of writing.

* BlkS: The size of a single block.
  This affects the size of a header.
  Only the lower 4 bits are used. The upper 4 bits are reserved.

  The size is encoded as `2^(x + 9)`.

* Ciph: Cipher algorithm to use to decrypt the header and records.

  All header data from byte 64 to the end of the block is encrypted.

.. table:: Cipher algorithms

  +----+----------+------------+
  | ID | Hash     | Encryption |
  +====+==========+============+
  |  0 | XXH3-128 | None       |
  +----+----------+------------+
  |  1 | Poly1305 | XChaCha12  |
  +----+----------+------------+

* KDF: The key derivation function to use to get the key necessary
  to decrypt the header.

.. table:: Key derivation functions

  +----+-----------+
  | ID | Algorithm |
  +====+===========+
  |  0 | None      |
  +----+-----------+
  |  1 | Argon2id  |
  +----+-----------+

* Key derivation function parameters: Parameters to use for the KDF.
  Contents depend on the selected KDF.

    .. table:: None

      +------+------+------+------+------+------+------+------+------+
      | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
      +======+======+======+======+======+======+======+======+======+
      |    8 |                                                       |
      +------+-------------------------------------------------------+

    * ID: is 0

    .. table:: Argon2id

      +------+------+------+------+------+------+------+------+------+
      | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
      +======+======+======+======+======+======+======+======+======+
      |    8 |             M             |  P   |          T         |
      +------+---------------------------+------+--------------------+

    * T: Iterations
    * M: Memory
    * P: Parallelism

    UID is used as the salt.

* Key hash: Hash of the key
  The hash is derived with Poly1305.

  * The message is 16 bytes of zeros.
  * The key is the derived key.
  * The hash is the 64 lower bits of the output.

* Nonce: Random integer used for encrypting the header [#]_.

  It is combined with the UID to form a 192-bit nonce.

.. [#] It is *critical* the nonce is never reused to prevent breaking stream
   ciphers, which are supposed to generate *one-time* pads.

   To demonstrate, suppose we have a plaintext `T` and a key `K` which
   generates one-time pad `P`.
   To encrypt `T`, it is xored with `P`, i.e. `E = T xor P`.
   Hence, if we have `T` and `E` we can derive P with `P = T xor E`.
   If the nonce is reused to encrypt a plaintext `E' = T' xor P` we can decrypt
   `E'` with `T' = E' xor P = E' xor (T xor E)`.

   A 64-bit nonce should be sufficient to ensure it is never reused.
   Even if the nonce is increased by 1 every nanosecond it would take
   584 years for it to repeat a previously used nonce.

* UID: Unique filesystem identifier.

* Hash: Hash of the header.
  The hash is calculated from encrypted data from byte 64 to the end of the
  header.

.. table:: Filesystem info

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |   64 |                     Configuration                     |
  +------+-------------------------------------------------------+
  |   72 |                   Total block count                   |
  +------+-------------------------------------------------------+
  |   80 |                      LBA offset                       |
  +------+-------------------------------------------------------+
  |   88 |                      Block count                      |
  +------+-------------------------------------------------------+
  |   96 |                                                       |
  +------+                                                       |
  |  104 |                                                       |
  +------+                        Key 1                          |
  |  112 |                                                       |
  +------+                                                       |
  |  120 |                                                       |
  +------+-------------------------------------------------------+
  |  128 |                                                       |
  +------+                                                       |
  |  136 |                                                       |
  +------+                        Key 2                          |
  |  144 |                                                       |
  +------+                                                       |
  |  152 |                                                       |
  +------+-------------------------------------------------------+
  |  160 |                   Object list root                    |
  +------+-------------------------------------------------------+
  |  168 |                  Object bitmap root                   |
  +------+-------------------------------------------------------+
  |  176 |                  Allocation log head                  |
  +------+-------------------------------------------------------+
  |  184 |                                                       |
  +------+                       Reserved                        |
  |  ... |                                                       |
  +------+-------------------------------------------------------+
  |  256 |                                                       |
  +------+                                                       |
  |  ... |              Free for use by filesystem               |
  +------+                                                       |
  |  504 |                                                       |
  +------+-------------------------------------------------------+

* Configuration: configuration values for the filesystem.

  .. table:: Configuration

    +------+------+------+------+------+------+------+------+------+
    | Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
    +======+======+======+======+======+======+======+======+======+
    |    0 |    Maximum record size    | Mirr. index | Mirr. count |
    +------+---------------------------+-------------+-------------+
    |    8 |     Compression level     |             | ObjLst Dpth |
    +------+---------------------------+-------------+-------------+
    |   16 |                 Compression algorithm                 |
    +------+-------------------------------------------------------+
    |   24 |                                                       |
    +------+                                                       |
    |   32 |                                                       |
    +------+                                                       |
    |   40 |                                                       |
    +------+                                                       |
    |   48 |                                                       |
    +------+                                                       |
    |   56 |                                                       |
    +------+-------------------------------------------------------+

    * Mirr. count: The amount of mirror volumes.
      Useful to determine how many mirrors should be waited for before allowing
      writes.

    * Mirr. index: The index of this chain in the mirror list.
      It simplifies loading code & prevents devices from being shuffled between
      chains on each mount.

    * Maximum record size: The maximum length of a record in bytes.

      The maximum record size is calculated as `2^(x + 9)`.

    * ObjLst Dpth: The depth of the object list tree.

    * Compression level: The compression level.
      The exact meaning depends on the compression algorithm, but usually
      higher means better but slower compression.

    * Compression algorithm: The default compression algorithm to use.

.. table:: Compression algorithms

  +----+-------------+
  | ID | Compression |
  +====+=============+
  |  0 | None        |
  +----+-------------+
  |  1 | LZ4         |
  +----+-------------+

* Total block count:
  The total amount of blocks this pool consists of.

* LBA offset: The offset to add to all LBAs on this disk.

* Block count: The amount of blocks in this pool.

* Key: The key to use when decrypting records [#]_.

.. [#] This key is different from the key used to encrypt the header so it is
   feasible to replace the key in case of a leak without reencrypting the
   entire filesystem.

* Bad block list head: List of block LBAs *on this device* that are known to be
  bad.

* Object list root: Record tree containing a list of objects.
  The length of the tree depends on ObjD.

* Object bitmap root: Record tree indicating whether an object is allocated.
  One bit is used per object.

.. [#] The bitmap allows much faster initialization of the object ID allocator.

* Reserved: unused space that is set aside for any potential updates to this
  specification.
  **Must** be zeroed.

* Free for use by filesystem: All space from byte 256 to the end of the block
  are free for use by the filesystem layer.


  When updating the headers, ensure the updates *do not* happen concurrently.
  That is, update all the start headers first, then the end headers.


Record
~~~~~~

A record is a single unit of data.
It consists of a header which is immediately followed by data.

The header fields other than the nonce are encrypted with Key 2.

.. table:: Record header
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                                                       |
  |    8 |                         Nonce                         |
  +------+                                                       |
  |   16 |                                                       |
  +------+---------------------------+---------------------------+
  |   24 |      Unpacked length      |       Packed length       |
  +------+---------------------------+---------------------------+
  |   32 |                                                       |
  +------+------+------------------------------------------------+
  |   40 | CAlg |                                                |
  +------+------+------------------------------------------------+
  |   48 |                                                       |
  +------+                         Hash                          |
  |   56 |                                                       |
  +------+-------------------------------------------------------+

* Nonce: Random integer used for encryption [#]_.

* Packed length: Length of the on-disk data in bytes.

* Unpacked length: Length of the data in bytes when unpacked.

* CAlg: The compression algorithm used on the data.

* Hash: The hash to verify the integrity of the *encrypted* data.

When packing data for storage, the following operations must be performed in
order:

1. Compression

2. Encryption with Key 1

   All blocks are encrypted as a whole, even if the tail is unused.

3. Hashing

   All blocks are hashed as a whole.

4. Header encryption with Key 2

The header itself is *excluded* from packing.


Record reference
~~~~~~~~~~~~~~~~

A record reference is a 64-bit value with a LBA and a block count.

.. table:: Record reference
  
  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                   LBA                   |    Blocks   |
  +------+-----------------------------------------+-------------+

* Blocks: the length of the record in blocks, including the header.

* LBA: the starting block address of the record.


Record tree
~~~~~~~~~~~

A record tree respresents a group of data.
If a tree has a depth greater than 0 it consists of multiple subtrees.

Some records may not unpack to the expected length.
The "missing" data is all zeroes [#]_.

.. [#] This optimization is called "zero-optimization" and is essential for
   sparse objects.


Object
~~~~~~

An object represents a collection of data.
It consists of multiple record trees.

.. table:: Object
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                         Root 0                        |
  +------+-------------------------------------------------------+
  |    8 |                         Root 1                        |
  +------+-------------------------------------------------------+
  |   16 |                         Root 2                        |
  +------+-------------------------------------------------------+
  |   24 |                         Root 3                        |
  +------+-------------------------------------------------------+

* Root 0 to 3: Record tree roots.
  The number indicates the depth of the record tree.


Object list
~~~~~~~~~~~

Objects are indexed by ID.

If the reference count of an object is greater than zero, it is in use.
Otherwise it is free.

Determining which slots are free is done by scanning the object bitmap [#]_.

.. [#] While scanning the object list directly is also possible, it is much
   faster to scan the bitmap.


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
where the first 8 bytes are a record reference pointing to the next node
and all bytes after it are log entries.
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
