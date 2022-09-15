File system
===========

Features
--------

* Out of band deduplication (+ copy-on-write).
* Arbitrarily large directories, indexed using a HTree variant.
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

A hash map/tree is used to keep track of files.

Every directory begins with a variable-sized byte header.

Header
+------+------+------+------+------+------+------+------+------+
| Byte |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 |  Extra len  |            Free block offset            |
+------+-------------+-----------------------------------------+
|    8 |  Entry len  |                                         |
+------+-------------+-----------------------------------------+
| N+16 |                      Extensions                       |
+------+-------------------------------------------------------+

Extensions define metadata to be attached to entries.
Each extension is prefixed with a 4 byte header.

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
|    0 | Flgs | KLen |     Key Offset or Next Table Length     |
+------+------+------+-----------------------------------------+
|    8 |           Object index or Next Table Offset           |
+------+-------------------------------------------------------+
|   16 |                     Extension data                    |
+------+-------------------------------------------------------+

If the object index or next table offset is 0, the entry is empty.

Flgs
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 | KorT |                  Type                          |
+------+------+------------------------------------------------+

KorT: 0 if key, 1 if next table.


Extensions
----------

UNIX 
~~~~

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

The embedded files extension allow storing small files directly in the
directory object, reducing space use and potentially speeding up loading of
small files.

Extension data
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
|    0 |      |           Embedded regular file type           |
+------+------+------------------------------------------------+
|    8 |      |          Embedded symbolic link type           |
+------+------+------------------------------------------------+

Entry data
+------+------+------+------+------+------+------+------+------+
| Bit  |    7 |    6 |    5 |    4 |    3 |    2 |    1 |    0 |
+======+======+======+======+======+======+======+======+======+
