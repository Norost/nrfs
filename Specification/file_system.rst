File system
===========

Features
--------

* Out of band deduplication (+ copy-on-write).
* Arbitrarily large directories, indexed using a hashmap.
* File names up to 255 bytes long.
* Extensions per directory.


File types
----------

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

Remaining IDs are free for use by extensions.


Copy-on-write
-------------

The reference count of an object is used to keep track of the amount of
pointers to it.
If a write is made to an object with a reference count higher than 1 a copy
will be made first.


Data structures
---------------

File
~~~~

A file has type 1.
It contains arbitrary data.


Directory
~~~~~~~~~

A directory is a special type of file that points to other files.

A directory has type 2.

A hashmap [#hashmap]_ is used to keep track of files.

.. [#hashmap] Hashmaps are used as they are very simple to implement, scale
   well and, contrary some expectations, perform well.
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
       This is not expected to be a problem win the general case, especially
       with a cryptographic hash.
     * Growing is slow, as it requires a full reallocation.
       This may result in performance hiccups when growing an extremely large
       directory, though this is not expected to be a problem for all but the
       largest directories (billions of entries).

Every directory begins with a variable-sized byte header.

Header
+------+------+------+------+------+------+------+------+------+
| Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 |  Header len |            Free block offset            |
+------+-------------+------+----------------------------------+
|    8 |  Entry len  | HLen | HAlg |        Entry count        |
+------+-------------+------+----------------------------------+
|   16 |                                                       |
+------+                          Key                          |
|   24 |                                                       |
+------+-------------------------------------------------------+
|   32 |                      Extensions                       |
+------+-------------------------------------------------------+
|  ... |                          ...                          |
+------+-------------------------------------------------------+

Extensions define metadata to be attached to entries.
Each extension is prefixed with a 4 byte header.

Hash algorithms are:

* 1: SipHash13

Extension header
+------+------+------+------+------+
| Byte |    3 |    2 |    1 |    0 |
+======+======+======+======+======+
|    0 |   Offset    | DLen | NLen |
+------+-------------+------+------+
|    4 |           Name            |
+------+---------------------------+
|  N+4 |           Data            |
+------+---------------------------+

Entry
+------+------+------+------+------+------+------+------+------+
| Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 | Type | KLen |               Key Offset                |
+------+------+------+-----------------------------------------+
|    8 |           Object index or Next Table Offset           |
+------+-------------------------------------------------------+
|   16 |                     Extension data                    |
+------+-------------------------------------------------------+
|  ... |                          ...                          |
+------+-------------------------------------------------------+

If the object index or next table offset is 0, the entry is empty.


Extensions
----------

UNIX 
~~~~

name: "unix"

The UNIX extension adds a 16 bit field to all entries.

Extension data
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+

Entry data
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 |   User WX   |     Group RWX      |     Global RWX     |
+------+------+------+----------------------------------+------+
|    8 |                                                | U. R |
+------+------------------------------------------------+------+


Embedded files
~~~~~~~~~~~~~~

name: "embedded"

The embedded files extension allow storing small files directly in the
directory object, reducing space use and potentially speeding up loading of
small files.

Extension data
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 |              Embedded regular file type               |
+------+-------------------------------------------------------+
|    8 |              Embedded symbolic link type              |
+------+-------------------------------------------------------+

Entry data
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 |                                                       |
+------+                        Length                         |
|    8 |                                                       |
+------+-------------------------------------------------------+
