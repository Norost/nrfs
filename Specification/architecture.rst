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


Cache object & entry states
---------------------------

In the cache, each entry can be in any of three states:

::

         /----------------<----------------\
         |                                 |
     +---+--+     +---------------+     +--+---+
  ->-+ None +--<--+ Busy  (ready) +-->--+ Idle |
     +---+--+     +-------+-------+     +--+---+
         |                |                |
         ^                ^                v
         |                |                |
         |       +--------+--------+       |
         \--->---+ Busy (wait mem) +-------/
                 +-----------------+

Every entry is in the None state by default.

When trying to use an entry memory must be reserved first.
Every entry hence transitions first to Busy (wait mem).
When the memory has been reserved the entry can move to the Busy (ready) state.

Entries in the Busy (ready) state are assumed to be maximally sized.
This simplifies modifying them as no extra memory needs to be reserved [#]_.

An entry can transition directly from the Idle to None state if it does not
need to be flushed.
If it does need to be flushed it can skip the Busy (ready) state if no task
needs the entry afterwards.

Entries that are in the Idle state are not currently being used.
They count towards both the hard and soft limit.
Their exact size is used for memory accounting as no task will attempt to
grow the entry.

To simplify the implementation, there are effectively two distinct state
machines:

* The first pertains to memory being reserved.

  ::

           /------------<------------\
           |                         |
       +---+---+     +-----+     +---+---+
    ->-+ Empty +-<->-+ Max +-<->-+ Exact |
       +-------+     +-----+     +-------+

* The second pertains to entry availability.

  ::

           /------------<-------------\
           |                          |
       +---+---+     +------+     +---+---+
    ->-+ None  +-->--+ Wait +-->--+ Ready |
       +-------+     +------+     +-------+


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


Directory
^^^^^^^^^
