import nose
from nose.tools import eq_

import os
import sqlalchemy as sq

from snakepit import hive, create

from snakepit.test.util import maketemp, assert_raises

class Create_Hive_Test(object):

    def test_simple(self):
        tmp = maketemp()
        db_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        hive_metadata = create.create_hive(db_uri)
        assert isinstance(hive_metadata, sq.MetaData)
        assert hive_metadata.bind is not None
        assert isinstance(hive_metadata.bind, sq.engine.Engine)
        eq_(str(hive_metadata.bind.url), db_uri)
        hive_metadata.bind.dispose()
        engine = sq.create_engine(db_uri)
        res = engine.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        got = res.fetchall()
        res.close()
        engine.dispose()
        got = [row[0] for row in got]
        eq_(
            sorted(got),
            sorted(hive.metadata.tables.keys()),
            )

    def test_repeat(self):
        tmp = maketemp()
        db_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        hive_metadata = create.create_hive(db_uri)
        hive_metadata.bind.dispose()
        hive_metadata = create.create_hive(db_uri)
        hive_metadata.bind.dispose()

class Create_Primary_Index_Test(object):

    def test_simple(self):
        tmp = maketemp()
        directory_uri = 'sqlite:///%s' % os.path.join(tmp, 'directory.db')

        directory_metadata = create.create_primary_index(
            directory_uri=directory_uri,
            dimension_name='frob',
            )
        assert isinstance(directory_metadata, sq.MetaData)
        assert directory_metadata.bind is not None
        assert isinstance(directory_metadata.bind, sq.engine.Engine)
        eq_(str(directory_metadata.bind.url), directory_uri)
        directory_metadata.bind.dispose()
        engine = sq.create_engine(directory_uri)
        res = engine.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        got = res.fetchall()
        res.close()
        engine.dispose()
        got = [row[0] for row in got]
        eq_(
            got,
            ['hive_primary_frob'],
            )

    def test_repeat(self):
        tmp = maketemp()
        directory_uri = 'sqlite:///%s' % os.path.join(tmp, 'directory.db')
        directory_metadata = create.create_primary_index(
            directory_uri=directory_uri,
            dimension_name='frob',
            )
        directory_metadata.bind.dispose()
        directory_metadata = create.create_primary_index(
            directory_uri=directory_uri,
            dimension_name='frob',
            )
        directory_metadata.bind.dispose()

class Create_Dimension_Test(object):

    def test_simple(self):
        tmp = maketemp()
        hive_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')

        hive_metadata = create.create_hive(hive_uri)
        dimension_id = create.create_dimension(hive_metadata, 'frob', 'fake-dir-uri')

        t = hive_metadata.tables['partition_dimension_metadata']
        q = sq.select(
            [
                t.c.id,
                t.c.name,
                t.c.index_uri,
                ],
            )
        res = q.execute()
        got = res.fetchall()
        res.close()
        hive_metadata.bind.dispose()
        eq_(len(got), 1)
        (got,) = got
        eq_(
            dict(got),
            dict(
                id=dimension_id,
                name='frob',
                index_uri='fake-dir-uri',
                ),
            )

    def test_repeat(self):
        tmp = maketemp()
        hive_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        hive_metadata = create.create_hive(hive_uri)
        create.create_dimension(
            hive_metadata,
            'frob',
            'fake-dir-uri',
            )
        e = assert_raises(
            create.DimensionExistsError,
            create.create_dimension,
            hive_metadata,
            'frob',
            'fake-dir-uri',
            )
        hive_metadata.bind.dispose()
        eq_(
            str(e),
            'Dimension exists already: %r' % 'frob',
            )


class Create_Node_Test(object):

    def test_simple(self):
        tmp = maketemp()
        hive_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        hive_metadata = create.create_hive(hive_uri)
        dimension_id = create.create_dimension(
            hive_metadata, 'frob', 'fake-dir-uri')
        node_id = create.create_node(
            hive_metadata=hive_metadata,
            dimension_id=dimension_id,
            node_name='node1',
            node_uri='fake-node-uri',
            )

        t = hive_metadata.tables['node_metadata']
        q = sq.select(
            [
                t.c.id,
                t.c.partition_dimension_id,
                t.c.name,
                t.c.uri,
                t.c.read_only,
                ],
            )
        res = q.execute()
        got = res.fetchall()
        res.close()
        hive_metadata.bind.dispose()
        eq_(len(got), 1)
        (got,) = got
        eq_(
            dict(got),
            dict(
                id=node_id,
                partition_dimension_id=dimension_id,
                name='node1',
                uri='fake-node-uri',
                read_only=False,
                ),
            )

    def test_repeat(self):
        tmp = maketemp()
        hive_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        hive_metadata = create.create_hive(hive_uri)
        dimension_id = create.create_dimension(
            hive_metadata, 'frob', 'fake-dir-uri')
        node_id = create.create_node(
            hive_metadata,
            'frob',
            'node1',
            'fake-node-uri',
            )
        e = assert_raises(
            create.NodeExistsError,
            create.create_node,
            hive_metadata,
            'frob',
            'node1',
            'fake-node-uri',
            )
        hive_metadata.bind.dispose()
        eq_(
            str(e),
            'Node exists already: %r' % 'node1',
            )
