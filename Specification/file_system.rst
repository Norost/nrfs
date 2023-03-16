File system
===========

Features
--------

* Out of band deduplication (+ copy-on-write).
* Up to `2^24 - 1` entries per directory
  * Optionally indexed with a hashmap.
* File names up to 255 bytes long.
* Extensions per directory.
* Embedding small files inside directories.


Partition identifier
--------------------

For GUID partition tables the following GUID is used to identify NRFS partitions:

::

  f752bf42-7b96-4c3a-9685-ad8497dca74c


Magic string
------------

The magic string in the NROS header is ``NRFS``.


File types
----------

.. table:: File types

  +------+-----------------------------+
  |  ID  |         Description         |
  +======+=============================+
  |    0 | Invalid / empty             |
  +------+-----------------------------+
  |    1 | Directory                   |
  +------+-----------------------------+
  |    2 | Regular file                |
  +------+-----------------------------+
  |    3 | Symbolic link               |
  +------+-----------------------------+
  |    4 | Embedded regular file       |
  +------+-----------------------------+
  |    5 | Embedded symbolic link      |
  +------+-----------------------------+

Remaining IDs are free for use by extensions.
Entries with unrecognized IDs may be shown but no other operations
may be performed on them.


Copy-on-write
-------------

The reference count of an object is used to keep track of the amount of
pointers to it.
If a write is made to an object with a reference count higher than 1 a copy
will be made first.


Deduplication
~~~~~~~~~~~~~

Tools can scan for duplicate files and make a CoW copy [#]_.

.. [#] On UNIX systems this is achieved with ``cp --reflink``.


Filesystem header
-----------------

The filesystem header contains:

* Root directory item information

* Enabled extensions.
  See the Extensions_ section for more information.

.. table:: Filesystem header

  ====== =====
  Offset Field
  ====== =====
       0 Root directory item data
      16 Extensions
  ====== =====
  

Embedded data
-------------

To reduce space usage and improve performance files with less than 64KiB of
data can be placed directly on a directory's heap.


Directory
---------

A directory is an object that contains a list of items.
Each item describes another object.

Directories may consist of multiple objects.
The main object contains the directory header and the item list.
Other objects are used for the heap and by extensions.

::

  +-----------+         +------+
  |           +---->----+ Heap |
  | Directory +-----+   +------+
  |           +--+  |
  +-----------+  |  |   +---------+
                 |  +->-+ Hashmap |
                 v      +---------+
                ...

Every directory begins with a 128 byte header.

.. table:: Directory header

  ====== =====
  Offset Field
  ====== =====
       0 Blocks used
       4 Highest block
       8 Heap ID
      16 Heap length
      24 Heap allocated
      32 Extensions
  ====== =====

* Blocks used: The total amount of blocks in use.

* Highest block: The highest block in use.

* Heap ID: ID of the heap object.

* Heap length: Highest byte allocated on the heap.

* Heap allocated: Total amount of bytes in use on the heap.

* Extensions: Extra attributes & other data defined in the object.

  If any extensions are not recognized, the directory *must* be treated as
  immutable.


Extensions
~~~~~~~~~~

.. table:: Directory extension

  ====== =====
  Offset Field
  ====== =====
       0 Idx
       1 Data
  ====== =====

* Idx: Index of the extension in the filesystem header, starting from 1.

  0 is reserved and is followed by no data.
  255 is reserved and must not be used.

* Data: Data associated with the extension.
  Length is defined in the filesystem header.


Item list
~~~~~~~~~

The item list is divided in blocks of 16 bytes each.
Blocks are chained to form a single item.
The low bits of the first byte of each block indicates whether the block is part
of a chain.

.. table:: Item block type

   =========== = = 
   Type / Bits 1 0
   =========== = =
   Name block  1 0
   Data block  x 1

Item
~~~~

An item describes a single object.
Each item starts with a name,
then data,
then additional data defined by extensions.

The first byte of a name block indicates the amount of name bytes.

.. table:: Name block byte

   ==== =====
   Bits Field
   ==== =====
    0:1 Type
      2 Not first
      3 Last
    4:7 Length

* Not first: Whether this block is *not* the first name block.

* Last: Whether this block is the last name block.

* Length: Amount of bytes constituting this block.
  Must be between 1 and 15.

After the name block(s) there is a single data block.
There are three formats for the data block.

.. table:: Item data for file & symlink types.

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                   Object ID                    | Type |
  +------+------------------------------------------------+------+
  |    8 |                        Length                         |
  +------+-------------------------------------------------------+

.. table:: Item data for embedded file & symlink types.

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                     Offset                     | Type |
  +------+-----------------------------------------+------+------+
  |    8 |                                         |   Length    |
  +------+-----------------------------------------+-------------+

.. table:: Item data for directory types.

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                   Object ID                    | Type |
  +------+------------------------------------------------+------+
  |    8 |                                                       |
  +------+-------------------------------------------------------+

* Type (high 7 bits): The type of the item.
  The value of the other data fields depend on the type.

* Object ID: The ID of the object.

* Offset: Offset of the data on the heap.

* Length: The length of the file or symlink in bytes.

After the data block comes an arbitrary amount of extension data.


Extensions
----------

Extensions specify additional functionality for directories.
Extension information is stored in the filesystem header.

.. table:: Extension header

  ====== =====
  Offset Field
  ====== =====
       0 ID
       1 NLen
       2 DLen
       3 FLen
       4 Name
  4+NLen FData
  ====== =====

* ID: ID of the extension.

* NLen: Length of the name of the extension.
  If 0, this header serves as padding.
  No extensions may appear after padding.

* DLen: Length of the directory item data associated with the extension.

* FLen: Length of FData

* Name: Name of the extension.

* FData: Additional data directly in the filesystem header.


UNIX
~~~~

name: "unix"

The UNIX extension adds a 16 bit field and 24-bit UID & GID to all entries.

.. table:: Directory header data

  +------+------+------+
  | Byte |    1 |    0 |
  +======+======+======+
  |    0 |   Offset    |
  +------+-------------+

.. table:: Item & filesystem header data

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |         GID        |         UID        | Permissions |
  +------+--------------------+--------------------+-------------+

.. table:: Permissions

  +------+------+------+------+------+------+------+------+------+
  | Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 | U. X |     Group RWX      |     Global RWX     |      |
  +------+------+--------------------+-------------+------+------+
  |    8 |                                         |   User RW   |
  +------+-----------------------------------------+-------------+


Modification time
~~~~~~~~~~~~~~~~~

name: "mtime"

The modification time extension adds a signed 63-bit time stamp to all entries.

It is expressed in microseconds, which gives it a range of ~242500 years.
The timestamp is relative to the UNIX epoch.

.. table:: Directory header data

  +------+------+------+
  | Byte |    1 |    0 |
  +======+======+======+
  |    0 |   Offset    |
  +------+-------------+

.. table:: Item & filesystem header data

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                       Timestamp                       |
  +------+-------------------------------------------------------+

**Note**: The low bit is unused.


Hashmap (WIP)
~~~~~~~~~~~~~

name: "hmap"

The hashmap [#]_ extension adds a data structure to speed up lookup operations.

It uses SipHash13 with Robin Hood hashing.

.. [#]

  Hashmaps are used as they are relatively simple to implement.
  They also scale and perform well.
  Two situations were considered:

  * A large directory is iterated.
  * A large directory where random entries are accessed.

  The following data structures were considered:

  * Plain array.
    These have notoriously poor performance in both cases.
  * BTree.
    These have good performance in general and are commonly used, but
    are relatively difficult to implement and suffer from indirection.
  * Hashmap. These have good performance in general.
    They are not commonly used as they require a contiguous region of storage.
    However, the underlying object storage makes this practical.
    The main drawbacks are:

    * O(n) worst-case lookup.
      This is not expected to be a problem in the general case, especially
      with a cryptographic hash.
    * Growing is slow, as it requires a full reallocation.
      This may result in performance hiccups when growing an extremely large
      directory, though this is not expected to be a problem for all but the
      largest directories (millions of entries).


.. table:: Extension data

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                          Key                          |
  |    8 |                                                       |
  +------+-----------------------------------------+-------------+
  |   16 |                 Offset                  | Properties  |
  +------+-----------------------------------------+-------------+

.. table:: Properties

  +------+------+------+------+------+------+------+------+------+
  | Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                    |           Hashmap size           |
  +------+--------------------+----------------------------------+
  |    8 |                                                       |
  +------+-------------------------------------------------------+

* Key: The key to use with the hash function.

* Hashmap size: The size of the hashmap as a power of 2.

* Offset: The offset of the hashmap in the directory object.


.. table:: Hashmap entry

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |               Hash               |     Item index     |
  +------+----------------------------------+--------------------+

* Hash: The lower 40 bits of the hash.

* Item index: the index of the corresponding directory item.
  This value is 1-based, i.e. index 1 refers to the first item.
  if the index is 0, the entry is unused.


Free list (WIP)
~~~~~~~~~~~~~~~

*TODO*


Examples
--------

Directory with "unix" & "mtime" extension
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*TODO*
