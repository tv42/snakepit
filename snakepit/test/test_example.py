# this is a reasonably straight-forward port of original
# ExampleHiveTest.java, without the ORMy bits

# This test provides a narrative example of how to create and use a
# hive. It goes through:
# -Installing the hive schema
# -Adding a data node
# -Installing the data schema
# -Adding a secondary index
# -Inserting a record into the data node
# -Retrieving the record by Primary Key
# -Retrieving the record by Secondary Index

# Author of the Java version was Britt Crawford (bcrawford@cafepress.com)

from nose.tools import eq_

import os
import sqlalchemy as sq

from snakepit import connect
from snakepit import create

from snakepit.test.util import maketemp

def test_createAndUseTheHive():
    # Install The Hive Metadata Schema
    tmp = maketemp()
    hive_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
    node_uri = 'sqlite:///%s' % os.path.join(tmp, 'aNode.db')

    hive_metadata = create.create_hive(hive_uri)
    hive_metadata.bind.dispose()

    # Load a Hive
    hive_metadata = connect.get_hive(hive_uri)

    # Create a Partition Dimension and add it to the Hive

    # We are going to partition our Product domain using the product
    # type string.
    dimension_name = 'ProductType'
    directory_metadata = create.create_primary_index(
        hive_uri,
        dimension_name,
        db_type='CHAR',
        )
    dimension_id = create.create_dimension(
        hive_metadata=hive_metadata,
        dimension_name=dimension_name,
        directory_uri=hive_uri,
        #TODO db_type=sq.String,
        )

    # Create a Data Node and add it to the partition dimension
    node_id = create.create_node(
        hive_metadata=hive_metadata,
        dimension_id=dimension_id,
        node_name='aNode',
        node_uri=node_uri,
        )

    # Make sure everything we just added actually got put into the
    # hive meta data.
    q = sq.select(
        [sq.func.count('*').label('count')],
        from_obj=[
            hive_metadata.tables['partition_dimension_metadata'],
            ],
        )
    res = q.execute().fetchone()
    assert res['count'] > 0

    t_part = hive_metadata.tables['partition_dimension_metadata']
    t_node = hive_metadata.tables['node_metadata']
    q = sq.select(
        [sq.func.count(t_node.c.id).label('count')],
        sq.and_(
            t_part.c.id==t_node.c.partition_dimension_id,
            t_part.c.name==dimension_name,
            ),
        )
    res = q.execute().fetchone()
    assert res['count'] > 0

    # Add a key, just to test.
    key = "knife";
    node_engine = connect.assign_node(
        hive_metadata=hive_metadata,
        dimension_name=dimension_name,
        dimension_value=key,
        )
    node_engine.dispose()

    # Just cleaning up the random key.

    # TODO I made this take node_name too, seemed like a more robust
    # API; for the purposes of this test, we know we only have one
    # node --tv
    connect.unassign_node(
        hive_metadata=hive_metadata,
        dimension_name=dimension_name,
        dimension_value=key,
        node_name='aNode',
        )

    # At this point there is no real data in the Hive just a directory
    # of Primary key to node mappings.
    generic_node_metadata = sq.MetaData()
    sq.Table(
        'products',
        generic_node_metadata,
        sq.Column('id', sq.Integer, primary_key=True),
        sq.Column('name', sq.String(255), nullable=False, unique=True),
        sq.Column('type', sq.String(255), nullable=False),
        )

    # First we need to load our data schema on to each data node.
    q = sq.select(
        [t_node.c.uri],
        sq.and_(
            t_node.c.partition_dimension_id==t_part.c.id,
            t_part.c.name==dimension_name,
            ),
        )
    res = q.execute().fetchall()
    for row in res:
        # TODO not supported yet by snakepit:
        # Ordinarily to get a connection to node from the hive we
        # would have to provide a key and the permissions (READ or
        # READWRITE) with which we want to acquire the connection.
        # However the getUnsafe method can be used [AND SHOULD ONLY BE
        # USED] for cases like this when there is no data yet loadedon
        # a node and thus no key to dereference.

        node_uri = row[t_node.c.uri]
        node_engine = sq.create_engine(node_uri)
        node_metadata = sq.MetaData()
        node_metadata.bind = node_engine
        for table in generic_node_metadata.tables.values():
            table.tometadata(node_metadata)
        node_metadata.create_all()
        node_metadata.bind.dispose()

    # Set up a secondary index on products so that we can query them
    # by name

    # First create a Resource and add it to the Hive. All Secondary
    # Indexes will be associated with this Resource.
    resource_name = 'Product'
#     create.create_secondary_index(
#         directory_uri=hive_uri,
#         resource_name=resource_name,
#         column_name='type', #TODO?
#         )

    t_resource = hive_metadata.tables['resource_metadata']
    r = t_resource.insert().execute(
        dimension_id=dimension_id,
        name=resource_name,
        db_type=0, #TODO sq.Integer,
        is_partitioning_resource=False,
        )
    (resource_id,) = r.last_inserted_ids()

    # Now create a SecondaryIndex
#     create.create_secondary_index(
#         directory_uri=hive_uri,
#         resource_name=resource_name,
#         column_name='name',
#         )

    # Add it to the Hive
#     create.add_secondary_index(
#         hive_metadata=hive_metadata,
#         resource_id=resource_id,
#         column_name='name',
#         #TODO db_type=sq.String,
#         )
    # Note: SecondaryIndexes are identified by
    # ResourceName.IndexColumnName

    # Now lets add a product to the hive.
    spork = dict(
        id=23,
        name='Spork',
        type='Cutlery',
        )

    # First we have to add a primary index entry in order to get
    # allocated to a data node.

    # While it is possible to write a record to multiple locations
    # within the Hive, the default implementation inserts a single
    # copy.
    node_engine = connect.assign_node(
        hive_metadata=hive_metadata,
        dimension_name=dimension_name,
        dimension_value=spork['type'],
        )

    # Next we insert the record into the assigned data node
    node_metadata = sq.MetaData()
    node_metadata.bind = node_engine
    for table in generic_node_metadata.tables.values():
        table.tometadata(node_metadata)
    node_metadata.tables['products'].insert(spork).execute()
    node_metadata.bind.dispose()

    # Update the resource id so that the hive can locate it
#     create.insert_resource_id(
#         hive_metadata=hive_metadata,
#         dimension_name=dimension_name,
#         resource_name=resource_name,
#         id=spork['id'],
#         pkey=spork['type'],
#         )

    # Finally we update the SecondaryIndex
#     connect.insert_secondary_index_key(
#         hive_metadata=hive_metadata,
#         dimension_name=dimension_name,
#         resource_name=resource_name,
#         column_name='name',
#         id=spork['name'],
#         pkey=spork['id'],
#         )

    # Retrieve spork by Primary Key
    node_engine = connect.get_engine(
        hive_metadata=hive_metadata,
        dimension_name=dimension_name,
        dimension_value=spork['type'],
        #TODO access=READ,
        )

    # Here I am taking advantage of the fact that I know there is only
    # one copy.
    node_metadata = sq.MetaData()
    node_metadata.bind = node_engine
    for table in generic_node_metadata.tables.values():
        table.tometadata(node_metadata)

    t = node_metadata.tables['products']
    q = sq.select(
        [
            t.c.id,
            t.c.name,
            ],
        t.c.id==spork['id'],
        limit=1,
        )
    res = q.execute().fetchone()
    assert res is not None
    product_a = dict(res)
    node_metadata.bind.dispose()

    # Make sure its a spork
    assert spork['name'] == product_a['name']

#     # Retrieve the spork by Name
#     node_engine = connect.get_engine_by_secondary(
#         hive_metadata=hive_metadata,
#         dimension_name=dimension_name,
#         resource_name=resource_name,
#         column_name='name',
#         id=spork['name'],
#         #TODO access=READ,
#         )

#     node_metadata = sq.MetaData()
#     node_metadata.bind = node_engine
#     for table in generic_node_metadata.tables.values():
#         table.tometadata(node_metadata)

#     t = node_metadata.tables['products']
#     q = sq.select(
#         [
#             t.c.id,
#             t.c.name,
#             ],
#         t.c.name==spork['name'],
#         limit=1,
#         )
#     res = q.execute().fetchone()
#     assert res is not None
#     product_b = dict(res)
#     node_metadata.bind.dispose()

#     # Make sure its a spork
#     eq_(spork['id'], product_b['id'])

#     # productA and productB are the same spork
#     eq_(product_a['id'], product_b['id'])
#     eq_(product_a['name'], product_b['name'])
