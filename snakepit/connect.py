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

def get_engine(hive_metadata, dimension_name, record_id):
    """
    Get node ID of hive node that stores C{record_id} for C{dimension_name}.

    @param hive_metadata: metadata for the hive db, bound to an engine

    @type hive_metadata: sqlalchemy.MetaData

    @param dimension_name: name of the dimension

    @type dimension_name: str

    @param record_id: the primary id of the row in question

    @type record_id: int

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
    t_primary = directory.dynamic_table(
        table=directory.hive_primary,
        directory_metadata=directory_metadata,
        name='hive_primary_%s' % dimension_name,
        )
    q = sq.select(
        [
            t_primary.c.node,
            ],
        t_primary.c.id==record_id,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchIdError(
            'dimension %r, record_id %r'
            % (dimension_name, record_id),
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

def create_record(hive_metadata, dimension_name):
    """
    Assign a node to record, insert record into directory, and return
    its C{record_id} and C{node_engine} connected to said node.

    @param hive_metadata: metadata for the hive db, bound to an engine

    @type hive_metadata: sqlalchemy.MetaData

    @param dimension_name: name of the dimension

    @type dimension_name: str

    @return: the primary id of the record in question and an engine
    connected to node storing it

    @rtype: tuple of (int, sqlalchemy.engine.Engine)
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
    t_primary = directory.dynamic_table(
        table=directory.hive_primary,
        directory_metadata=directory_metadata,
        name='hive_primary_%s' % dimension_name,
        )
    r = t_primary.insert().execute(
        node=node_id,
        secondary_index_count=0,
        last_updated=datetime.datetime.now(),
        read_only=False,
        )
    (record_id,) = r.last_inserted_ids()

    node_engine = sq.create_engine(
        node_uri,
        strategy='threadlocal',
        )
    return (record_id, node_engine)
