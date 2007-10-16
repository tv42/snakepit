import nose
from nose.tools import eq_

import os
import sqlalchemy as sq

from snakepit import create, connect

from snakepit.test.util import maketemp, assert_raises

class Get_Hive_Test(object):

    def test_simple(self):
        tmp = maketemp()

        hive_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        hive_metadata = create.create_hive(
            hive_uri=hive_uri,
            )
        dimension_id = create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            directory_uri=hive_uri,
            )
        hive_metadata.bind.dispose()

        hive_metadata = connect.get_hive(
            hive_uri=hive_uri,
            )
        t = hive_metadata.tables['partition_dimension_metadata']
        got = t.select().execute().fetchall()
        got = [dict(row) for row in got]
        #TODO
        for row in got: del row['db_type']
        eq_(
            got,
            [
                dict(
                    id=dimension_id,
                    name='frob',
                    index_uri=hive_uri,
                    ),
                ],
            )
        hive_metadata.bind.dispose()

class Get_Engine_Test(object):

    def test_simple(self):
        tmp = maketemp()

        p42_metadata = sq.MetaData()
        p42_metadata.bind = sq.create_engine(
            'sqlite:///%s' % os.path.join(tmp, 'p42.db'),
            strategy='threadlocal',
            )
        t_frob = sq.Table(
            'frob',
        p42_metadata,
            sq.Column('id', sq.Integer, primary_key=True),
            sq.Column('xyzzy', sq.Integer, nullable=False),
            )
        p42_metadata.create_all()

        hive_metadata = create.create_hive(
            'sqlite:///%s' % os.path.join(tmp, 'hive.db'))
        directory_metadata = create.create_primary_index(
            'sqlite:///%s' % os.path.join(tmp, 'directory.db'),
            'frob',
            )
        dimension_id = create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            directory_uri=str(directory_metadata.bind.url),
            )
        directory_metadata.bind.dispose()
        node_id = create.create_node(
            hive_metadata=hive_metadata,
            dimension_id=dimension_id,
            node_name='node42',
            node_uri=str(p42_metadata.bind.url),
            )
        (record_id, node_engine) = connect.create_record(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            )
        node_engine.dispose()

        got = connect.get_engine(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            record_id=record_id,
            )
        assert isinstance(got, sq.engine.Engine)
        eq_(str(got.url), str(p42_metadata.bind.url))
        got.dispose()
        hive_metadata.bind.dispose()
        p42_metadata.bind.dispose()

    def test_bad_dimension(self):
        tmp = maketemp()
        hive_metadata = create.create_hive(
            'sqlite:///%s' % os.path.join(tmp, 'hive.db'))
        create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='these-are-nt-the-droids',
            directory_uri='fake',
            )
        e = assert_raises(
            connect.NoSuchDimensionError,
            connect.get_engine,
            hive_metadata=hive_metadata,
            dimension_name='frob',
            record_id=123,
            )
        eq_(
            str(e),
            'No such dimension: %r' % 'frob',
            )
        hive_metadata.bind.dispose()

    def test_bad_id(self):
        tmp = maketemp()
        hive_metadata = create.create_hive(
            'sqlite:///%s' % os.path.join(tmp, 'hive.db'))
        directory_metadata = create.create_primary_index(
            'sqlite:///%s' % os.path.join(tmp, 'directory.db'),
            'frob',
            )
        dimension_id = create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            directory_uri=str(directory_metadata.bind.url),
            )
        create.create_node(
            hive_metadata=hive_metadata,
            dimension_id=dimension_id,
            node_name='node42',
            node_uri='sqlite://',
            )
        (record_id, node_engine) = connect.create_record(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            )
        node_engine.dispose()
        directory_metadata.bind.dispose()
        got = connect.get_engine(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            # make it wrong to trigger the error
            record_id=record_id+1,
            )
        assert got is None
        hive_metadata.bind.dispose()

    def test_bad_node(self):
        tmp = maketemp()

        hive_metadata = create.create_hive(
            'sqlite:///%s' % os.path.join(tmp, 'hive.db'))
        directory_metadata = create.create_primary_index(
            directory_uri='sqlite:///%s' % os.path.join(tmp, 'directory.db'),
            dimension_name='frob',
            )
        dimension_id = create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            directory_uri=str(directory_metadata.bind.url),
            )
        directory_metadata.bind.dispose()
        node_id = create.create_node(
            hive_metadata=hive_metadata,
            dimension_id=dimension_id,
            node_name='node34',
            node_uri='sqlite://',
            )
        (record_id, node_engine) = connect.create_record(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            )
        node_engine.dispose()
        hive_metadata.tables['node_metadata'].delete().execute()
        hive_metadata.bind.dispose()

        e = assert_raises(
            connect.NoSuchNodeError,
            connect.get_engine,
            hive_metadata,
            'frob',
            record_id,
            )
        eq_(
            str(e),
            'No such node: dimension %r, node_id %d' \
                % ('frob', node_id)
            )


class CreateRecord_Test(object):

    def test_simple(self):
        tmp = maketemp()

        p42_metadata = sq.MetaData()
        p42_metadata.bind = sq.create_engine(
            'sqlite:///%s' % os.path.join(tmp, 'p42.db'),
            strategy='threadlocal',
            )
        t_frob = sq.Table(
            'frob',
            p42_metadata,
            sq.Column('id', sq.Integer, primary_key=True),
            sq.Column('xyzzy', sq.Integer, nullable=False),
            )
        p42_metadata.create_all()

        directory_metadata = create.create_primary_index(
            directory_uri='sqlite:///%s' % os.path.join(tmp, 'directory.db'),
            dimension_name='frob',
            )
        hive_metadata = create.create_hive(
            'sqlite:///%s' % os.path.join(tmp, 'hive.db'))

        dimension_id = create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            directory_uri=str(directory_metadata.bind.url),
            )
        directory_metadata.bind.dispose()
        create.create_node(
            hive_metadata=hive_metadata,
            dimension_id=dimension_id,
            node_name='node42',
            node_uri=str(p42_metadata.bind.url),
            )

        got = connect.create_record(hive_metadata, 'frob')
        assert isinstance(got, tuple)
        eq_(len(got), 2)
        record_id, node_engine = got
        eq_(record_id, 1)
        assert isinstance(node_engine, sq.engine.Engine)
        eq_(str(node_engine.url), str(p42_metadata.bind.url))

    def test_bad_no_node(self):
        tmp = maketemp()

        directory_metadata = create.create_primary_index(
            'sqlite:///%s' % os.path.join(tmp, 'directory.db'),
            'frob',
            )
        hive_metadata = create.create_hive(
            'sqlite:///%s' % os.path.join(tmp, 'hive.db'))

        dimension_id = create.create_dimension(
            hive_metadata=hive_metadata,
            dimension_name='frob',
            directory_uri=str(directory_metadata.bind.url),
            )
        node_id = create.create_node(
            hive_metadata=hive_metadata,
            # make it wrong to trigger the error
            dimension_id=dimension_id+1,
            node_name='node42',
            node_uri='fake',
            )

        e = assert_raises(
            connect.NoNodesForDimensionError,
            connect.create_record,
            hive_metadata,
            'frob',
            )
        eq_(
            str(e),
            'No nodes found for dimension: %r' % 'frob',
            )
