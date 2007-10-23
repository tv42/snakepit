import sqlalchemy as sq

metadata = sq.MetaData()

semaphore_metadata = sq.Table(
    'semaphore_metadata',
    metadata,
    # this is int in HiveConfigurationSchema.java
    sq.Column('read_only', sq.Boolean, nullable=False),
    sq.Column('revision', sq.Integer, nullable=False),
    )

node_metadata = sq.Table(
    'node_metadata',
    metadata,
    sq.Column('id', sq.Integer, primary_key=True),
    sq.Column('partition_dimension_id', sq.Integer,
              sq.ForeignKey('partition_dimension_metadata.id'),
              nullable=False,
              ),
    sq.Column('name', sq.String(255), nullable=False),
    sq.Column('uri', sq.String(255), nullable=False),
    # this is int in HiveConfigurationSchema.java
    sq.Column('read_only', sq.Boolean),

    # TODO what's the scope of the name? I guess you could
    # say name is hostname, and same machine can serve two
    # dimensions -- then name isn't unique alone, only in
    # combination with partition_dimension_id
    sq.UniqueConstraint('partition_dimension_id', 'name'),
    )

partition_dimension_metadata = sq.Table(
    'partition_dimension_metadata',
    metadata,
    sq.Column('id', sq.Integer, primary_key=True),
    sq.Column('name', sq.String(64), nullable=False, unique=True),
    sq.Column('index_uri', sq.String(255), nullable=False),
    sq.Column('db_type', sq.String(64), nullable=False),
    )

secondary_index_metadata = sq.Table(
    'secondary_index_metadata',
    metadata,
    sq.Column('id', sq.Integer, primary_key=True),
    sq.Column('resource_id', sq.Integer,
              sq.ForeignKey('resource_metadata.id'),
              nullable=False,
              ),
    sq.Column('column_name', sq.String(64), nullable=False),
    sq.Column('db_type', sq.String(64), nullable=False),

    sq.UniqueConstraint('resource_id', 'column_name'),
    )

resource_metadata = sq.Table(
    'resource_metadata',
    metadata,
    sq.Column('id', sq.Integer, primary_key=True),
    sq.Column('dimension_id', sq.Integer,
              sq.ForeignKey('partition_dimension_metadata.id'),
              nullable=False,
              ),
    sq.Column('name', sq.String(128), nullable=False),
    sq.Column('db_type', sq.String(64), nullable=False),
    sq.Column('is_partitioning_resource', sq.Boolean,
              nullable=False,
              default=True),

    # TODO unique on just combo of dimension_id, name?
    sq.UniqueConstraint('name'),
    )

