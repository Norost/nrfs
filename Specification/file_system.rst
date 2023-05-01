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

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0     16 Root directory item data
      16     16 Attribute keys directory
      32      8 Attribute ID to key map
  ====== ====== =====


Embedded data
-------------

To reduce space usage and improve performance files with less than 64KiB of
data can be placed directly on a directory's heap.


Attribute keys
--------------

Attribute keys are shared between all directories.
They are stored in a hidden directory.

  The key map reuses the directory structure to reduce code duplication.

.. table:: Attribute key data

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      8 ID
        8      8 Reference count
   ====== ====== =====

ID to key mappings use a plain list in another object.

.. table:: Attribute ID to key

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0      6 Tag
   ====== ====== =====


Directory
---------

Directories use the key-value store defined in ``key_value_store.rst``.

Header
~~~~~~

The directory header is stored in the 32 bytes of user data of the key-value
store.

.. table:: Directory header

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      8 Old directory
  ====== ====== =====

* Old directory: Old directory data is being transferred from.

  Only valid if not -1.

* Length: Length of the key.

  If 0, it is the end of the attribute key list.

* Hash: Low 8 bits of the SipHash13 of the attribute key.

Item
~~~~

An item describes a single object.

.. table:: Item

   ====== ====== =====
   Offset Length Field
   ====== ====== =====
        0     16 Data
       16      6 Attribute list offset
       22      2 Attribute list length
   ====== ====== =====

.. table:: Embedded file / symlink data

   ============= ============= =====
   Offset (bits) Length (bits) Field
   ============= ============= =====
               0             3 Type
              16            48 Offset
              64            16 Length
              96            16 Capacity
   ============= ============= =====

.. table:: File / symlink data

   ============= ============= =====
   Offset (bits) Length (bits) Field
   ============= ============= =====
               0             3 Type
               3            61 Object ID
              64            64 Length
   ============= ============= =====

.. table:: Directory data

   ============= ============= =====
   Offset (bits) Length (bits) Field
   ============= ============= =====
               0             3 Type
               3            61 Object ID
               64           32 Item count
   ============= ============= =====


Item attributes
~~~~~~~~~~~~~~~

.. table:: Attribute value if length < 2555

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      I ID
       I      1 Length
     1+I      N Value
  ====== ====== =====

.. table:: Attribute value if length == 255

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      I ID
       I      1 255
     1+I      2 Length
     3+I      6 Offset
  ====== ====== =====

ID is encoded as a variable-length little-endian integer.
The high bit indicates whether the next byte is part of the integer.

  Examples: 0x20 = 32, 0x80 0x01 = 128


Standard attributes
-------------------

Modification time
~~~~~~~~~~~~~~~~~

name: "nrfs.mtime"

The modification time attribute adds a signed time stamp.
The length is variable.

The timestamp is relative to the UNIX epoch.


UID
~~~

name: "nrfs.uid"

This attribute adds a user ID.
The length is variable.


GID
~~~

name: "nrfs.gid"

This attribute adds a group ID.
The length is variable.


UNIX
~~~~

name: "nrfs.unixmode"

The UNIX mode attribute adds a mode field.
It is at least 2 bytes long.
The first 9 bits indicate global, group and user permissions respectively.
Other bits are reserved.

.. table:: UNIX attribute permissions

  ============= ============= =====
  Offset (bits) Length (bits) Field
  ============= ============= =====
              0             3 Global RWX
              3             3 Group RWX
              6             3 User RWX
  ============= ============= =====
