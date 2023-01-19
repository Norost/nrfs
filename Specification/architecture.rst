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

Resizing a record tree may cause its height to change.
However, the tree on disk still has the old height, so one cannot trivially
replace or substitute the root node.

When resizing there are two cases to consider:

* Growing, where the depth may increase and records must be added.
* Shrinking, where the depth may decrease and records must be removed.

If the depth increases a single record is added on top of the current root
record.
When this record is flushed it will automatically create a new record on top of
it until the new root is reached.

If the depth decreases all records outside the current range are either removed
or trimmed.
The current root is immediately moved to the new root.
While this root record may include redundant data and be redundantly flushed
this will automatically be resolved and should not be an issue in practice.
To aid this process `Pseudo-objects`_ are used.


Cache object & entry states
---------------------------

In the cache, each object & entry can be in any of four states:

::

       +-------------+         +-------------+
       |             |         |             |
  -->--+ Not present +---->----+  Fetching   |
       |             |         |   (Busy)    |
       +------+------+         +------+------+
              |                       |
              ^                       v
              |                       |
       +------+------+         +------+------+
       |             +----<----+             |
       |  Flushing   |         |   Present   |
       |   (Busy)    +---->----+             |
       +-------------+         +-------------+

Every entry is in the "not present" state by default.

Entries that are being flushed are inaccessible for reading or writing.
This simplifies the flushing logic & should have little to no impact on
performance as an entry is flushed when it is either:

* Being evicted, in which case it likely will not be accessed soon anyways.
* Being flushed without eviction, which may happen during transaction commit
  during which no other operations may take place.

The root of objects are also cached alongside the entries for each object and
are subject to the same mechanism.

To simplify things, Flushing and Fetching are combined into a single "Busy"
state.


Pseudo-objects
--------------




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

::

  +----------
  |
