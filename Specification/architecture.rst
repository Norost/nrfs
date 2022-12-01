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
  +-----------------------+-----------------------+   +-----------+
                          |                 
  +-----------------------+-----------------------+   +--------------+
  |                     Cache                     +---+ Record tree* |
  +------+----------------+----------------+------+   +--------------+
         |                |                |
  +------+------+  +------+------+  +------+------+
  | Record Tree |  | Record Tree |  | Record Tree |
  +------+------+  +------+------+  +------+------+


Record tree*
^^^^^^^^^^^^

The implementation of record trees is not as trivial as may seem from the
graph.
In particular, one must be careful with writes as naively updating every record
from leaf to root on every write requires an excessive amount of hashing and
compression to create the records themselves.

To deal with this issue, record trees *on top* of the cache layer should only
update leaf records.
Parent records will remain unchanged.
When the cache needs to evict dirty records, it will instantiate a record tree
itself, which then updates parent records accordingly.


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
