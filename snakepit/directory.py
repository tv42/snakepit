"""
HiveDB client access via SQLAlchemy
"""

import sqlalchemy as sq

metadata = sq.MetaData()

DB_TYPES = dict(
    BIGINT=sq.Integer,
    CHAR=sq.String,
    DATE=sq.DateTime,
    DOUBLE=sq.Integer,
    FLOAT=sq.Float,
    INTEGER=sq.Integer,
    SMALLINT=sq.SmallInteger,
    TIMESTAMP=sq.DateTime,
    TINYINT=sq.SmallInteger,
    VARCHAR=sq.String,
    )

hive_primary = sq.Table(
    'hive_primary_DIMENSION',
    metadata,
    # the 'id' column is added dynamically, with type based on
    # partition_dimension_metadata.db_type
    sq.Column('node', sq.SmallInteger,
              nullable=False,
              index=True,
              ),
    sq.Column('secondary_index_count', sq.Integer, nullable=False),
    # Hive_ERD.png says "date", but I think you want time too
    sq.Column('last_updated', sq.DateTime,
              nullable=False,
              index=True,
              ),
    sq.Column('read_only', sq.Boolean, nullable=False, default=False),
    )

hive_secondary = sq.Table(
    'hive_secondary_RESOURCE_COLUMN',
    metadata,
    # TODO this should be whatever datatype
    # secondary_index_metadata.db_type says, no uniqueness guarantee
    sq.Column('id', sq.Integer,
              nullable=True,
              index=True,
              ),
    # TODO this should be whatever datatype resource_metadata.db_type
    # says; this doesn't point to primary index but to the column
    # named by secondary_index_metadata.column_name in the table named
    # by resource_metadata.name
    sq.Column('pkey', sq.Integer,
              nullable=False,
              index=True,
              ),
    )

def dynamic_table(table, directory_metadata, name):
    """
    Access C{table} under new C{directory_metadata} with new C{name}.
    """
    new = directory_metadata.tables.get(name, None)
    if new is not None:
        return new

    new = sq.Table(
        name,
        directory_metadata,
        *[c.copy() for c in table.columns])
    return new

def get_primary_table(
    directory_metadata,
    dimension_name,
    db_type,
    ):
    table_name = 'hive_primary_%s' % dimension_name
    table = dynamic_table(
        table=metadata.tables['hive_primary_DIMENSION'],
        directory_metadata=directory_metadata,
        name=table_name,
        )
    table.append_column(
        sq.Column(
            'id',
            DB_TYPES[db_type],
            nullable=False,
            ),
        )
    #table.constraints.add(sq.UniqueConstraint('id', 'node'))
    return table
