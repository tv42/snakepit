==================================
 Snakepit -- HiveDB Python client
==================================

	Because hives are for ants and bees.

.. note::

	`Snakepit` is my interpretation of the ideas behind HiveDB_.
	It does aim to be compatible with the real HiveDB, at least
	for now. It also will probably aim to be as simple as
	possible, at least for now.

	There's plenty of things that have not been implemented yet,
	many features of HiveDB are currently unimplemented and I'm
	sure I've misunderstood plenty of details.

	The API *will* change as I gather experience on programming
	things with the library.

`Snakepit` is a horizontally partitioned, scalable, database access
library. It manages the partitioning for you, while trying to be light
and staying out of the way -- in concrete terms, you tell it what
record id you're interested in, and it hands you an SQLAlchemy_ engine
pointing to the node storing that record. From there on, you're free
to access the record, and all things you know to be stored on the same
node; e.g. a user and their blog entries could always be on the same
node.

It's licensed under the `MIT license`_.

You can get ``snakepit`` via ``git`` by saying::

    git clone git://eagain.net/snakepit

And install it via::

    python setup.py install

Though you may want to use e.g. ``--prefix=``. For Debian/Ubuntu
users, the source will probably be debianized later (TODO).


Setting up
==========

HiveDB docs aren't in a great shape either, and that project has a lot
more experience with the database schema. I'm briefly summarizing what
I know, and then you're on your own:

- a `hive` is the central point of coordination (and often a SPOF), it
  stores information about the nodes in the system

- a `directory` stores the ``id`` to ``node`` mapping, it can reside
  in the same database as the `hive`

- a `node` is a database that actually stores the data

Records are partitioned to nodes based on `partitioning
dimensions`. This can happen by primary key or secondary key. For
example, users could be partitioned to nodes and their blog posts
could be stored on the same node. In such a schema, ``user_id`` would
be mapped to a ``node`` in the directory, and ``blogpost_id`` would be
mapped to ``user_id``, via a secondary index, also in the directory.


Using it
========

There's a couple of sample apps: ``demo.sh`` and ``demo.py``.

Actual use of the library is still.. shall we say.. finding it's path.

Good luck on your adventures. You can reach me at tv@eagain.net, or
as Tv on irc.freenode.net etc.



.. Links:

.. _HiveDB: http://www.hivedb.org/

.. _SQLAlchemy: http://www.sqlalchemy.org/

.. _`MIT license`: LICENSE

