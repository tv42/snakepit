import datetime
import random
import sqlalchemy as sq

from snakepit import hive, directory

class NoSuchDimensionError(Exception):
    """No such dimension"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

class NoSuchIdError(Exception):
    """No such id"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

class NoSuchNodeError(Exception):
    """No such node"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

def get_hive(hive_uri):
    """
    Open hive at C{hive_uri} and return metadata.

    @param hive_uri: dburi where the hive metadata is stored

    @type hive_uri: str

    @return: A metadata connected to the hive database. Caller is
    responsible for disposing of the engine with
    C{metadata.bind.dispose()}.

    @rtype: sqlalchemy.MetaData
    """
    hive_metadata = sq.MetaData()
    hive_metadata.bind = sq.create_engine(
        hive_uri,
        strategy='threadlocal',
        )
    for table in hive.metadata.tables.values():
        table.tometadata(hive_metadata)
    return hive_metadata

def get_engine(hive_metadata, dimension_name, dimension_value):
    """
    Get node ID of hive node that stores C{dimension_value} for
    C{dimension_name}.

    @param hive_metadata: metadata for the hive db, bound to an engine

    @type hive_metadata: sqlalchemy.MetaData

    @param dimension_name: name of the dimension

    @type dimension_name: str

    @param dimension_value: value for this dimension

    @type dimension_value: something matching assumptions set by
    partition_dimension_metadata.db_type

    @return: engine connected the the node

    @rtype: sqlalchemy.engine.Engine
    """

    t = hive_metadata.tables['partition_dimension_metadata']
    q = sq.select(
        [
            t.c.id,
            t.c.index_uri,
            ],
        t.c.name==dimension_name,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchDimensionError(repr(dimension_name))
    partition_id = res[t.c.id]
    index_uri = res[t.c.index_uri]

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        index_uri,
        strategy='threadlocal',
        )
    t_primary = directory.get_primary_table(
        directory_metadata=directory_metadata,
        dimension_name=dimension_name,
        db_type='INTEGER',
        )
    q = sq.select(
        [
            t_primary.c.node,
            ],
        t_primary.c.id==dimension_value,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchIdError(
            'dimension %r, dimension_value %r'
            % (dimension_name, dimension_value),
            )
    node_id = res[t_primary.c.node]

    t = hive_metadata.tables['node_metadata']
    q = sq.select(
        [
            t.c.uri,
            ],
        sq.and_(
            t.c.id==node_id,
            t.c.partition_dimension_id==partition_id,
            ),
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchNodeError(
            'dimension %r, node_id %d' % (dimension_name, node_id))
    node_uri = res[t.c.uri]

    node_engine = sq.create_engine(
        node_uri,
        strategy='threadlocal',
        )
    return node_engine

class NoNodesForDimensionError(Exception):
    """No nodes found for dimension"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

def _pick_node(hive_metadata, dimension_name, partition_id):
    t = hive_metadata.tables['node_metadata']
    q = sq.select(
        [
            t.c.id,
            ],
        t.c.partition_dimension_id==partition_id,
        )
    res = q.execute().fetchall()
    try:
        res = random.choice(res)
    except IndexError:
        raise NoNodesForDimensionError(repr(dimension_name))
    node_id = res[t.c.id]
    return node_id

def assign_node(hive_metadata, dimension_name, dimension_value):
    """
    Assign a node for this value of the dimension.

    @param hive_metadata: metadata for the hive db, bound to an engine

    @type hive_metadata: sqlalchemy.MetaData

    @param dimension_name: name of the dimension

    @type dimension_name: str

    @param dimension_value: value for this dimension

    @type dimension_value: something matching assumptions set by
    partition_dimension_metadata.db_type

    @return: engine connected to node storing C{dimension_value}

    @rtype: sqlalchemy.engine.Engine
    """
    t = hive_metadata.tables['partition_dimension_metadata']
    q = sq.select(
        [
            t.c.id,
            t.c.index_uri,
            ],
        t.c.name==dimension_name,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchDimensionError(repr(dimension_name))
    partition_id = res[t.c.id]
    index_uri = res[t.c.index_uri]

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        index_uri,
        strategy='threadlocal',
        )
    t_primary = directory.get_primary_table(
        directory_metadata=directory_metadata,
        dimension_name=dimension_name,
        db_type='INTEGER',
        )

    def primary_index_get_or_insert(conn):
        q = sq.select(
            [t_primary.c.node],
            t_primary.c.id==dimension_value,
            bind=conn,
            for_update=True,
            )
        res = q.execute().fetchone()
        if res is not None:
            # it's already in there, we're done!
            node_id = res[t_primary.c.node]
            return node_id

        # node not assigned yet, insert while inside this transaction
        # so the above for_update will hold it locked for us. ugh
        # locks.
        node_id = _pick_node(
            hive_metadata=hive_metadata,
            dimension_name=dimension_name,
            partition_id=partition_id,
            )
        q = sq.insert(
            t_primary,
            {
                t_primary.c.id: dimension_value,
                t_primary.c.node: node_id,
                t_primary.c.secondary_index_count: 0,
                t_primary.c.last_updated: datetime.datetime.now(),
                t_primary.c.read_only: False,
                },
            )        # important to do this within the transaction
        conn.execute(q)
        return node_id

    node_id = directory_metadata.bind.transaction(
        primary_index_get_or_insert)
    directory_metadata.bind.dispose()

    t = hive_metadata.tables['node_metadata']
    q = sq.select(
        [
            t.c.uri,
            ],
        t.c.id==node_id,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise RuntimeError(
            'Node disappeared from under us: just added node_id=%r to'
            ' partition dimension %r' % (node_id, dimension_name))
    node_uri = res[t.c.uri]

    node_engine = sq.create_engine(
        node_uri,
        strategy='threadlocal',
        )
    return node_engine


class NoSuchNodeForDimensionValueError(Exception):
    """Node not found for dimension value"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

def unassign_node(
    hive_metadata,
    dimension_name,
    dimension_value,
    node_name,
    ):
    """
    Unassign node for this value of the dimension.

    After this call, that node will no longer be asked to serve this
    dimension value.

    @param hive_metadata: metadata for the hive db, bound to an engine

    @type hive_metadata: sqlalchemy.MetaData

    @param dimension_name: name of the dimension

    @type dimension_name: str

    @param dimension_value: value for this dimension

    @type dimension_value: something matching assumptions set by
    partition_dimension_metadata.db_type

    @param node_name: name of the node to remove

    @type node_name: str
    """
    t = hive_metadata.tables['partition_dimension_metadata']
    q = sq.select(
        [
            t.c.id,
            t.c.index_uri,
            ],
        t.c.name==dimension_name,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchDimensionError(repr(dimension_name))
    partition_id = res[t.c.id]
    index_uri = res[t.c.index_uri]

    t = hive_metadata.tables['node_metadata']
    q = sq.select(
        [
            t.c.id,
            ],
        sq.and_(
            t.c.partition_dimension_id==partition_id,
            t.c.name==node_name,
            ),
        )
    res = q.execute().fetchone()
    if not res:
        raise NoNodesForDimensionError(repr(dimension_name))
    node_id = res[t.c.id]

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        index_uri,
        strategy='threadlocal',
        )
    t_primary = directory.get_primary_table(
        directory_metadata=directory_metadata,
        dimension_name=dimension_name,
        db_type='INTEGER',
        )
    q = t_primary.delete(
        sq.and_(
            t_primary.c.id==dimension_value,
            t_primary.c.node==node_id,
            # TODO t_primary.c.secondary_index_count==0?
            # TODO t_primary.c.read_only==False?
            ),
        )
    res = q.execute()
    if res.rowcount < 1:
        raise NoSuchNodeForDimensionValueError(
            'dimension %r value %r, node name %r'
            % (
                dimension_name,
                dimension_value,
                node_name,
                ),
            )

    directory_metadata.bind.dispose()
