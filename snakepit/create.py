import sqlalchemy as sq

from snakepit import hive

def create_hive(hive_uri):
    """
    Create hive at C{hive_uri}

    @return: A metadata connected to the hive database. Caller is
    responsible for disposing of the engine with
    C{metadata.bind.dispose()}.

    @rtype: sqlaclhemy.MetaData
    """
    hive_metadata = sq.MetaData()
    hive_metadata.bind = sq.create_engine(
        hive_uri,
        strategy='threadlocal',
        )
    for table in hive.metadata.tables.values():
        table.tometadata(hive_metadata)
    hive_metadata.create_all()
    return hive_metadata
