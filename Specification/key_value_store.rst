Key-value store
===============

The key-value store maps names up to 255 bytes long to items with arbitrary
data and metadata.

Features
--------

* Metadata
* Embedded data
* HashDoS resistant

Data structures
---------------

The header is offset by a user-configurable amount of bytes.

.. table:: Header

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0     16 Hash key
      16      6 Used
      22      6 Free head
      28     12 Free region 0
      40     12 Free region 1
      52     12 Free region 2
      64 6*4096 HAMT root
  ====== ====== =====

.. table:: Free region

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      6 Offset
       0      6 Length
  ====== ====== =====

.. table:: HAMT entry

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      6 Offset
  ====== ====== =====

Item data is offset by a user-configurable amount of bytes.

.. table:: Item

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0   6*16 HAMT table
    8*16      1 Name length
  1+8*16      N Name
  ====== ====== =====

.. table:: Region

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      8 Marker
       8      N Data
       N      8 Marker
  ====== ====== =====

.. table:: Marker

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      2 Flags
       2      6 Length
  ====== ====== =====

N is a multiple of 16.

Flag bit 0 is 1 for used regions, 0 for free regions.
