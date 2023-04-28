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
      28 6*4096 HAMT root
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
