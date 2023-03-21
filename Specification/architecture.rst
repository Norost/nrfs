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


Caching
-------

The fundamental idea to efficient caching is to make changes to records
**bubble up**.
i.e. always start from the bottom of the tree and work upwards.

For example, take the following record tree with dirty leaves (marked by a
``*``):

::

       o
       |
       F
      / \
     D   E
    / \   \
   A*  B*  C

When ``A`` and ``B`` are flushed, the data is packed and new records are
created.
The parent node is updated with these new nodes and leads to the following
state (``'`` indicates a new record):

::

       o
       |
       F
      / \
     D*  E
    / \   \
   A'  B'  C

This process is repeated when the parent node ``D`` is flushed:

::

       o
       |
       F*
      / \
     D'  E
    / \   \
   A'  B'  C

When ``F`` is flushed, the location of the *root* record depends on what the
record tree is used for:

* If the record tree represents an object, the root record is put in one of the
  leaves of the object list.
  The object list is also a record tree, so the process repeats.
* If the record tree represents the object list, the root record is put in the
  filesystem header.

  Aside from making writes efficient, this also indirectly leads to transaction
  semantics.

  It may happen that a parent node is flushed despite having dirty descendants.
  While this may lead to redundant I/O traffic this is not expected to be a
  problem in practice and may even be beneficial as it frees memory for other
  more immediate purposes.

Resizing
........

Since resizing record trees while allowing concurrent reads & writes has proven
to be exceptionally difficult & error-prone the trees used by objects cannot
be resized.

The trees used by the object list and bitmap can be resized however, since:

* growing the tree merely involves moving the root to a new record, then
  waiting for the new record to be flushed.
* shrinking does not require zeroing any records, as by the time shrinking
  is possible all the unused objects & bits are already zeroed.

Memory use accounting
.....................

Various schemes have been attempted to accurately gauge memory usage,
but in the end a simple scheme that assumes all records use the same
amount of memory regardless of real size was adopted.

While this will lead to the cache being vastly underutilized in many cases,
the cache of the VFS and possibly the (DMA) cache of the disk drivers should
compensate for this.

Hard limit
``````````

The hard limit ensures the cache won't use up all system memory under heavy
load. When there is no more room to insert an entry, a task blocks until memory
is available again.

Soft limit
``````````

The soft limit is the target size of the cache.
If exceeded tasks won't block but entries will begin being evicted in the
background.

The soft limit must be strictly lower than the hard limit to ensure there is
always room for new entries.


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


Filesystem
~~~~~~~~~~

Since the filesystem has been built on top of the object store, which already
implements a cache, it has been designed to be as "cacheless", i.e. no cached
data about each file/directory/... must be explictly kept around.
This is to simplify implementations.

In particular, when reading data the VFS can cache it more efficiently than the
filesystem driver.
When writing data another caching layer on top of the object store is unlikely
to be useful as data is written through anyways.

Directory
^^^^^^^^^

The specification of the directory itself has been kept as minimal as
practically viable to allow incremental improvements with extensions while
avoid legacy baggage as much as possible.
