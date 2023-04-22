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

.. table:: Header

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0     16 Hash key
      16      8 Old key-value store
      24      8 Used
      32      8 Free head
      40      1 User data length
      64 6*4096 HAMT root
  ====== ====== =====

.. table:: HAMT entry

  ====== ====== =====
  Offset Length Field
  ====== ====== =====
       0      6 Offset
  ====== ====== =====

.. table:: Item

  ======== ====== =====
  Offset   Length Field
  ======== ====== =====
         0   6*16 HAMT table
      8*16      D User data
    8*16+D      1 Name length
  8*16+D+1      N Name
  ======== ====== =====
