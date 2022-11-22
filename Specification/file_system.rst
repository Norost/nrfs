File system
===========

Features
--------

* Out of band deduplication (+ copy-on-write).
* Up to 2^32 entries per directory, indexed using a hashmap.
* File names up to 255 bytes long.
* Extensions per directory.
* Embedding small files inside directories.


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
  |    1 | Regular file                |
  +------+-----------------------------+
  |    2 | Directory                   |
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

A directory is a special type of file that points to other files.

It consists of two objects: one object with a header and hashmap at ID
and one object for "heap" data at ID + 1 [#]_

.. [#]

  The map and heap are split so the map can grow without needing to shift the
  heap data or leave large holes.
  Fixing the heap ID relative to the map's ID allows loading it concurrently.

A hashmap [#]_ is used to keep track of files.

.. [#]

  Hashmaps are used as they are very simple to implement.
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

Every directory begins with a variable-sized byte header.

.. table:: Header
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |        Entry count        | MLen | HAlg | ELen | HLen |
  +------+---------------------------+------+------+------+------+
  |    8 |                                                       |
  +------+                          Key                          |
  |   16 |                                                       |
  +------+-------------------------------------------------------+
  |   24 |                      Extensions                       |
  +------+-------------------------------------------------------+
  |  ... |                          ...                          |
  +------+-------------------------------------------------------+

HLen and ELen are in units of 8 bytes.
MLen represents a power of 2.

Extensions define metadata to be attached to entries.
Each extension is prefixed with a 4 byte header.

Hash algorithms are [#]_:

* 0: No hash
* 1: SipHash13 with Robin Hood hashing

.. [#]

   If the hashing algorithm isn't known the table can still be iterated as a
   fallback (i.e. assume "No hash").

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

Hashmap entry
~~~~~~~~~~~~~

.. table:: Entry header if KLen <= 16
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |              Key (0 to 5)               | KLen | Type |
  +------+-----------------------------------------+------+------+
  |    8 |                     Key (6 to 15)                     |
  +------+-------------------------------------------------------+

.. table:: Entry header if KLen > 16
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |               Key offset                | KLen | Type |
  +------+---------------------------+-------------+------+------+
  |    8 |                           |           Hash            |
  +------+---------------------------+---------------------------+

* Type: The type of the entry.
  If 0, it is empty / invalid.

* KLen: The length of the key.

* Key: The key string.
  Only valid if KLen is 16 or less [#]_.

* Key offset: Pointer to the key in the heap

* Hash: The 32-bit hash of the key.
  Only valid if KLen is larger than 16.

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


.. table:: Regular entry
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                        Header                         |
  |    8 |                                                       |
  +------+-------------------------------------------------------+
  |   16 |                       Object ID                       |
  +------+-------------------------------------------------------+
  |   24 |                    Extension data                     |
  +------+-------------------------------------------------------+
  |  ... |                          ...                          |
  +------+-------------------------------------------------------+

.. table:: Embedded entry
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                                                       |
  +------+                        Header                         |
  |    8 |                                                       |
  +------+-------------+-----------------------------------------+
  |   16 | Data Length |               Data offset               |
  +------+-------------+-----------------------------------------+
  |   24 |                    Extension data                     |
  +------+-------------------------------------------------------+
  |  ... |                          ...                          |
  +------+-------------------------------------------------------+

* Object index: The ID of the corresponding object.
  Only valid if the type is 1, 2 or 3.

* Data offset: The offset of the entry's data in the heap.
  Only valid if the type is 4 or 5.

* Data length: The offset of the entry's data in the heap.
  Only valid if the type is 4 or 5.

* Extension data: Optional metadata associated with the entry.
  See Extensions_.


Allocation log
~~~~~~~~~~~~~~

After the hashmap comes an allocation log.
Each entry in the log indicates a single allocation or deallocation.

.. table:: Log entry
  :align: center
  :widths: grid

  +------+------+------+------+------+------+------+------+------+
  | Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
  +======+======+======+======+======+======+======+======+======+
  |    0 |                        Offset                         |
  +------+-------------------------------------------------------+
  |    8 |                        Length                         |
  +------+-------------------------------------------------------+

The high bit of length indicates whether the entry is an allocation (0)
or deallocation (1).

The size of the log is determined by the total size of the map object.


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

It is expressed in milliseconds, which gives it a range of ~584 million years.
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
