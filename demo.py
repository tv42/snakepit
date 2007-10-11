#!/usr/bin/python
from snakepit import create

hive_uri = 'sqlite:///hive.db'
dimension_name = 'frob'
directory_uri = hive_uri

hive_metadata = create.create_hive(
    hive_uri=hive_uri,
    )
directory_metadata = create.create_primary_index(
    directory_uri=directory_uri,
    dimension_name=dimension_name,
    )
create.create_dimension(
    hive_metadata=hive_metadata,
    dimension_name=dimension_name,
    directory_uri=directory_uri,
    )
create.create_node(
    hive_metadata,
    dimension_name,
    'node1',
    'sqlite:///node1.db',
    )
create.create_node(
    hive_metadata,
    dimension_name,
    'node2',
    'sqlite:///node2.db',
    )

directory_metadata.bind.dispose()
hive_metadata.bind.dispose()
