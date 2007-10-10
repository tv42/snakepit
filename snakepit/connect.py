import sqlalchemy as sq

from snakepit import directory

class NoSuchDimensionError(Exception):
    """No such dimension"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

class NoSuchNodeError(Exception):
    """No such node"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

def get_engine(metadata, dimension, id_):
    """
    Get node ID of hive node that stores C{id_} for C{dimension}.

    @param metadata: metadata for the hive db, bound to an engine

    @type metadata: sqlalchemy.MetaData

    @param dimension: name of the dimension

    @type dimension: str

    @param id_: the primary id of the row in question

    @type id_: int
    """

    t = metadata.tables['partition_dimension_metadata']
    q = sq.select(
        [
            t.c.id,
            t.c.index_uri,
            ],
        t.c.name==dimension,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        raise NoSuchDimensionError(repr(dimension))
    partition_id = res[t.c.id]
    index_uri = res[t.c.index_uri]

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        index_uri,
        strategy='threadlocal',
        )
    t_primary = directory.dynamic_table(
        table=directory.hive_primary,
        metadata=directory_metadata,
        name='hive_primary_%s' % dimension,
        )
    q = sq.select(
        [
            t_primary.c.node,
            ],
        t_primary.c.id==id_,
        limit=1,
        )
    res = q.execute().fetchone()
    if res is None:
        return None
    node_id = res[t_primary.c.node]

    t = metadata.tables['node_metadata']
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
        raise NoSuchNodeError('dimension %r, node_id %d' % (dimension, node_id))
    node_uri = res[t.c.uri]

    node_engine = sq.create_engine(
        node_uri,
        strategy='threadlocal',
        )
    return node_engine
