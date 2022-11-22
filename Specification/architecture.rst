Architecture
============

::

  +---------+  +---------+  +---------+
  | Storage |  | Storage |  | Storage |
  +----+----+  +----+----+  +----+----+
       |            |            |
  +----+------------+------------+----+
  |           Object store            |
  +-----------------+-----------------+
                    |
  +-----------------+-----------------+
  |            Filesystem             |
  +-----------------------------------+

Object store
~~~~~~~~~~~~

::

  +-----------------------------------------------+   +-----------+
  |                 Record store                  +---+ Allocator |
  +------+----------------+----------------+------+   +-----------+
         |                |                |
  +------+------+  +------+------+  +------+------+
  | Record tree |  | Record tree |  | Record tree |
  +------+------+  +------+------+  +------+------+
         |                |                |
  +------+----------------+----------------+------+
  |                     Cache                     |
  +-----------------------------------------------+


Resilvering
^^^^^^^^^^^

Resilvering is the process of copying data from one mirror pair to another.
This process is asynchronous.

First the header blocks are zeroed to avoid accidental mounting in case of
interruption (e.g. power loss).
The portion of the allocation log that corresponds to the resilvered device is copied.
All allocations in this copy are transferred between the pairs.
Any writes that are made in the meantime are kept track in a separate dirty map.
When the transfer has finished, the copy is replaced with the dirty map.
This process is repeated until the dirty map is empty or sufficiently small to
warrant a brief write stall.
Finally, the headers are written.
