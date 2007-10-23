import sqlalchemy as sq

from snakepit import directory
from snakepit import connect

def create_hive(hive_uri):
    """
    Create hive at C{hive_uri}.

    @return: A metadata connected to the hive database. Caller is
    responsible for disposing of the engine with
    C{metadata.bind.dispose()}.

    @rtype: sqlaclhemy.MetaData
    """
    hive_metadata = connect.get_hive(hive_uri)
    hive_metadata.create_all()
    return hive_metadata

def create_primary_index(
    directory_uri,
    dimension_name,
    ):
    """
    Create a primary index for C{dimension_name} at C{directory_uri}.
    """
    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        directory_uri,
        strategy='threadlocal',
        )
    table = directory.dynamic_table(
        table=directory.metadata.tables['hive_primary_DIMENSION'],
        directory_metadata=directory_metadata,
        name='hive_primary_%s' % dimension_name,
        )
    directory_metadata.create_all()
    return directory_metadata


class DimensionExistsError(Exception):
    """Dimension exists already"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

def create_dimension(hive_metadata, dimension_name, directory_uri):
    """
    Create a dimension with C{dimension_name} at C{hive_metadata},
    where the directory index is stored at C{directory_uri}.

    Directory index must be set up before calling this function.

    @return: id of created dimension

    @rtype: int

    @raise DimensionExistsError: a dimension with that name exists
    already in this hive
    """
    t = hive_metadata.tables['partition_dimension_metadata']
    try:
        r = t.insert().execute(
            name=dimension_name,
            index_uri=directory_uri,
            db_type=0, # TODO
            )
    except sq.exceptions.SQLError, e:
        # sqlalchemy 0.3.x is hiding details and not providing a
        # db-independent abstraction for what the error actually
        # was, so we need to resort to kludges

        # only catch sq.exceptions.IntegrityError when it's safe
        # to depend on sqlalchemy 0.4

        # http://www.sqlalchemy.org/trac/ticket/706
        if 'IntegrityError' in unicode(e):
            raise DimensionExistsError(repr(dimension_name))
        else:
            raise

    (dimension_id,) = r.last_inserted_ids()
    r.close()
    return dimension_id

class NodeExistsError(Exception):
    """Node exists already"""

    def __str__(self):
        return ': '.join([self.__doc__]+list(self.args))

def create_node(hive_metadata, dimension_id, node_name, node_uri):
    """
    Create a node with in dimension having C{dimension_id} with
    C{node_name} at C{hive_metadata}, where the records are stored at
    C{node_uri}.

    Node must be set up before calling this function.

    @return: id of created node

    @rtype: int

    @raise NoSuchDimensionError: no such dimension found

    @raise NodeExistsError: a node with that name exists
    already in this hive
    """
    t = hive_metadata.tables['node_metadata']
    try:
        r = t.insert().execute(
            partition_dimension_id=dimension_id,
            name=node_name,
            uri=node_uri,
            read_only=False, # TODO
            )
    except sq.exceptions.SQLError, e:
        # sqlalchemy 0.3.x is hiding details and not providing a
        # db-independent abstraction for what the error actually
        # was, so we need to resort to kludges

        # only catch sq.exceptions.IntegrityError when it's safe
        # to depend on sqlalchemy 0.4

        # http://www.sqlalchemy.org/trac/ticket/706
        if 'IntegrityError' in unicode(e):
            raise NodeExistsError(repr(node_name))
        else:
            raise

    (node_id,) = r.last_inserted_ids()
    r.close()
    return node_id
