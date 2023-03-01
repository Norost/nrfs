File system
===========

Features
--------

* Out of band deduplication (+ copy-on-write).
* Up to `2^32 - 1` entries per directory, indexed using a hashmap.
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
  :align: center
  :widths: grid

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
Entries with unrecognized IDs may be shown and moved but no other operations
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


Embedded data
-------------

To reduce space usage and improve performance files with less than 64KiB of
data can be placed directly on a directory's heap.


Directory
---------

A directory is an object that contains or contains pointers to other objects.

Every directory begins with a variable-sized byte header.

.. table:: Header
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |          Allocation log offset          | ILen | HLen |
  +------+-----------------------------------------+------+------+
  |    8 |            Group list offset            | Group count |
  +------+-----------------------------------------+-------------+
  |   16 |                                                       |
  +------+                      Extensions                       |
  |  ... |                                                       |
  +------+-------------------------------------------------------+

* HLen: Length of the directory header, in units of 8 bytes.

* ILen: Length of each item, in units of 8 bytes.

* Allocation log offset: Offset in the directory object of the allocation log.

* Group count: The size of the group list.

* Group list offset: Offset in the directory object of the group list.

* Extensions: Extra attributes & other data defined in the object.

  If any extensions are not recognized, the directory *must* be treated as
  immutable.


Extensions
~~~~~~~~~~

.. table:: Extension header
  :align: center
  :widths: grid

  +------+------+------+
  | Byte |    1 |    0 |
  +======+======+======+
  |    0 | DLen | NLen |
  +------+------+------+
  |    2 |    Name     |
  +------+-------------+
  |  N+2 |    Data     |
  +------+-------------+

* NLen: Length of the name of the extension.

* DLen: Length of the data associated with the extension.

* Name: Name of the extension.

* Data: Data associated with the extension.


Directory group
~~~~~~~~~~~~~~~

Every group has a size of ``32 + item_len * 256`` bytes.

.. table:: Group pointer

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |             |                 Offset                  |
  +------+-------------+-----------------------------------------+

* Offset: Offset in the directory object to the group.
  If 0, the group is not allocated.

.. table:: Group

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                                                       |
  |    8 |                                                       |
  +------+                        Bitmap                         |
  |   16 |                                                       |
  +------+                                                       |
  |   24 |                                                       |
  +------+-------------------------------------------------------+
  |   32 |                                                       |
  +------+                         Items                         |
  |  ... |                                                       |
  +------+-------------------------------------------------------+

* Bitmap: Bitmap indicated used item slots.
  1 means used.

* Items: List of items.

Directory item
~~~~~~~~~~~~~~

Each item has a fixed length, defined in the directory header.

.. table:: Item

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                         Name                          |
  |    8 |                                                       |
  +------+-------------------------------------------------------+
  |   16 |                                                       |
  +------+                         Data                          |
  |   24 |                                                       |
  +------+-------------------------------------------------------+
  |   32 |                                                       |
  +------+                       Metadata                        |
  |  ... |                                                       |
  +------+-------------------------------------------------------+

* Name: The name of the item.

  .. table:: Item name if NLen <= 15

    +------+------+------+------+------+------+------+------+------+
    | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
    +======+======+======+======+======+======+======+======+======+
    |    0 |                 Name (0 to 6)                  | NLen |
    +------+------------------------------------------------+------+
    |    8 |                    Name (7 to 14)                     |
    +------+-------------------------------------------------------+

  .. table:: Item name if NLen > 15
    :align: center
    :widths: grid

    +------+------+------+------+------+------+------+------+------+
    | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
    +======+======+======+======+======+======+======+======+======+
    |    0 |                 Name (0 to 6)                  | NLen |
    +------+-----------------------------------------+------+------+
    |    8 |              Name offset                | Name (7-9)  |
    +------+-----------------------------------------+-------------+

  * NLen: The length of the key.
    If it 0, the item is unused.

  * Name: The key string.
    Bytes 7 to 14 are only valid if NLen is 15 or less [#]_.

  * Name offset: Pointer to the key in the heap
    Only valid if NLen is larger than 15.

  .. [#]

    Embedding the key avoids an indirection.

    The maximum length of the embedded key is based on data from a Devuan
    desktop:

    * Total amount of files: 18094927

    ================ ======= ================ ============
    File name length  Count  Cumulative count Cumulative %
    ================ ======= ================ ============
                   1   47985            47986         0.27
                   2  292412           340398         1.88
                   3  271133           611531         3.38
                   4  383093           994624         5.50
                   5 1459539          2454163        13.56
                   6 4328975          6783138        37.49
                   7  797426          7580564        41.89
                   8 1324312          8904876        49.21
                   9 1129762         10034638        55.46
                  10  726535         10761173        59.47
                  11  818181         11579354        63.99
                  12  718414         12297768        67.96
                  13  518331         12816099        70.83
                  14  504373         13320472        73.61
                  15  422600         13743072        75.95
                  16  381073         14124145        78.06
                  17  375204         14499349        80.13
                  18  450636         14949985        82.62
                  19  284422         15234407        84.19
                  20  248121         15482528        85.56
    ================ ======= ================ ============

    Some bytes of the key are kept embedded even with NLen > 15 to speed up
    lookups.

* Data: Data associated with the item.

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
    |    0 |                 Offset                  |      | Type |
    +------+-----------------------------------------+------+------+
    |    8 |                                         |   Length    |
    +------+-----------------------------------------+-------------+

  .. table:: Item data for directory types.

    +------+------+------+------+------+------+------+------+------+
    | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
    +======+======+======+======+======+======+======+======+======+
    |    0 |                   Object ID                    | Type |
    +------+----------------------------------+-------------+------+
    |    8 |                                  |     Item count     |
    +------+----------------------------------+--------------------+

  * Type: The type of the item.
    The value of the other data fields depend on the type.

  * Object ID: The ID of the object.

  * Length: The length of the file or symlink in bytes.

  * Item count: The amount of items in the directory.

* Metadata: Metadata associated with the item.
  The contents & length of this field depends on the extensions defined in the
  directory header.
  See _Extensions.

Hashmap entry
~~~~~~~~~~~~~


Allocation log
~~~~~~~~~~~~~~

After the hashmap comes an allocation log.
Each entry in the log indicates a single allocation or deallocation.

.. table:: Heap log entry
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                        Offset                         |
  +------+-------------------------------------------------------+
  |    8 |                        Length                         |
  +------+-------------------------------------------------------+

.. table:: Heap log entry
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |          Length           |          Offset           |
  +------+---------------------------+---------------------------+

Each log entry inverts the status of the range covered (i.e. ``xor``).
Each log entry indicates either an allocation or deallocation,
never both partially.
The length of each entry may never be 0.

The size of the log is determined by the total size of the map object.

Unallocated regions **must** be zeroed [#]_.

.. [#] Requiring unallocating regions to be zeroed improves compression
   efficiency and simplifies implementations.


Extensions
----------

UNIX
~~~~

name: "unix"

The UNIX extension adds a 16 bit field and 24-bit UID & GID to all entries.

.. table:: Extension data
  :align: center
  :widths: grid

  +------+------+------+
  | Byte |    1 |    0 |
  +======+======+======+
  |    0 |   Offset    |
  +------+-------------+

.. table:: Entry data
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |         GID        |         UID        | Permissions |
  +------+--------------------+--------------------+-------------+

.. table:: Permissions
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |   User WX   |     Group RWX      |     Global RWX     |
  +------+-------------+--------------------+-------------+------+
  |    8 |                                                | U. R |
  +------+------------------------------------------------+------+


Modification time
~~~~~~~~~~~~~~~~~

name: "mtime"

The modification time extension adds a signed 64-bit time stamp to all entries.

It is expressed in microseconds, which gives it a range of ~585000 years.
The timestamp is relative to the UNIX epoch.

.. table:: Extension data
             :align: center
  :widths: grid

  +------+------+------+
  | Byte |    1 |    0 |
  +======+======+======+
  |    0 |   Offset    |
  +------+-------------+

.. table:: Entry data
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                       Timestamp                       |
  +------+-------------------------------------------------------+


Hashmap
~~~~~~~

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
  :align: center
  :widths: grid

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
  :align: center
  :widths: grid

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
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |               Hash               |     Item index     |
  +------+----------------------------------+--------------------+

* Hash: The lower 40 bits of the hash.

* Item index: the index of the corresponding directory item.
  This value is 1-based, i.e. index 1 refers to the first item.
  if the index is 0, the entry is unused.
