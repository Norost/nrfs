File system
===========

Features
--------

* Out of band deduplication (+ copy-on-write).
* Up to `2^24 - 1` entries per directory
  * Optionally indexed with a hashmap.
* File names up to 255 bytes long.
* Arbitrary attributes.
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

  == ===========
  ID Description
  == ===========
   0 Invalid
   1 Directory
   2 Regular file
   3 Symbolic link
   4 Embedded regular file
   5 Embedded symbolic link
  == ===========


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

The item list is a variant of a HTree, with a variable depth and no probing.

Directory space is allocated in chunks of 1KiB.

Header
~~~~~~

Every directory begins with a 1KiB header.

.. table:: Directory header

  ====== =====
  Offset Field
  ====== =====
       0 Hash key
      16 Chunks used
      20 Highest chunk
      24 New directory
  ====== =====

* Hash key: Secret key used with SipHash13.

* Chunks used: The total amount of chunks in use.

* Highest chunk: The highest chunk in use.

* New directory: New directory data is being transferred to.

  Only valid if not -1.

HTree
~~~~~

The HTree contains references to all items in the directory.
The root is located directly after the directory header.

When inserting an item, the name is hashed.
The hash is split into chunks of 10 bits.
The lowest 8 bits are used as index into the root node.
If the slot is empty, the item is inserted.
If the slot points to a child node, the next 8 bits are used as index in the
child node.
If an item is already present at the index, a new parent node is inserted and
the conflicting item is moved downwards.
This process repeats until an empty slot is found or no more hash bits are
left.

SipHash13 with a random key is used to provide resistance against HashDOS.

.. table:: HTree entry

   ============= ============= =====
   Offset (bits) Length (bits) Field
   ============= ============= =====
               0             1 Is parent
               1            31 Chunk
   ============= ============= =====


Item
~~~~

An item describes a single object.

.. table:: Object

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      2 Chunks
        2      1 Name length
        3      1 Type
        4      4 Attribute list length
        8      N Name
      8+N      A Attributes
    8+N+A      D Data
   ====== ====== =====

* Chunks: Chunks occupied by this node.

* Name length

  If 0, the item has no name and is dangling,
  i.e. not referenced by the HTree.

* Type

* Attribute list length: Length of the attribute list in bytes.

* Name

* Attributes

* Data

.. table:: Item data for file & symlink types.

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      8 Object ID
        8      8 Length
   ====== ====== =====

.. table:: Item data for embedded file & symlink types.

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      3 Length
   ====== ====== =====

.. table:: Item data for directory types.

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      8 Object ID
   ====== ====== =====

* Object ID: The ID of the object.

* Offset: Offset of the data on the heap.

* Length: The length of the file or symlink in bytes.

After the data block comes an arbitrary amount of extension data.


Standard attributes
-------------------

UNIX
~~~~

name: "nrfs.unix"

The UNIX extension adds 16-bit permissions and 24-bit UID & GID to all entries.

.. table:: UNIX attribute data

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      2 Permissions
        2      3 UID
        3      6 GID
   ====== ====== =====

.. table:: UNIX attribute permissions

  ============= ============= =====
  Offset (bits) Length (bits) Field
  ============= ============= =====
              0             3 Global RWX
              3             3 Group RWX
              6             3 User RWX
  ============= ============= =====


Modification time
~~~~~~~~~~~~~~~~~

name: "nrfs.mtime"

The modification time extension adds a signed 64-bit time stamp to all entries.

It is expressed in microseconds, which gives it a range of ~585000 years.
The timestamp is relative to the UNIX epoch.
