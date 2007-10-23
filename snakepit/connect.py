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

    t = hive_metadata.tables['node_metadata']
    q = sq.select(
        [
            t.c.id,
            t.c.uri,
            ],
        t.c.partition_dimension_id==partition_id,
        )
    res = q.execute().fetchall()
    try:
        res = random.choice(res)
    except IndexError:
        raise NoNodesForDimensionError(repr(dimension_name))
    node_id = res[t.c.id]
    node_uri = res[t.c.uri]

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
    t_primary.insert().execute(
        id=dimension_value,
        node=node_id,
        secondary_index_count=0,
        last_updated=datetime.datetime.now(),
        read_only=False,
        )
    directory_metadata.bind.dispose()

    node_engine = sq.create_engine(
        node_uri,
        strategy='threadlocal',
        )
    return node_engine
