"""
HiveDB client access via SQLAlchemy
"""

import sqlalchemy as sq

metadata = sq.MetaData()

hive_primary = sq.Table(
    'hive_primary_DIMENSION',
    metadata,
    sq.Column('id', sq.Integer, primary_key=True),
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

    sq.UniqueConstraint('id', 'node'),
    )

hive_secondary = sq.Table(
    'hive_secondary_RESOURCE_COLUMN',
    metadata,
    sq.Column('id', sq.Integer,
              nullable=True,
              index=True,
              ),
    sq.Column('pkey', sq.Integer,
              sq.ForeignKey("hive_primary_TODO.id"),
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
