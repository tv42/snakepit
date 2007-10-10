import nose
from nose.tools import eq_

import os
import datetime
import sqlalchemy as sq

from snakepit import hive, directory, connect

from snakepit.test.util import maketemp, assert_raises

def test_simple():
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

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'directory.db'),
        strategy='threadlocal',
        )
    pri = directory.dynamic_table(
        table=directory.hive_primary,
        metadata=directory_metadata,
        name='hive_primary_frob',
        )
    pri.create()
    pri.insert().execute(
        id=123,
        node=42,
        secondary_index_count=0,
        last_updated=datetime.datetime.now(),
        read_only=False,
        )

    hive_metadata = sq.MetaData()
    hive_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'hive.db'),
        strategy='threadlocal',
        )
    for table in hive.metadata.tables.values():
        table.tometadata(hive_metadata)
    hive_metadata.create_all()

    t = hive_metadata.tables['partition_dimension_metadata']
    t.insert().execute(
        id=13,
        name='frob',
        index_uri=str(directory_metadata.bind.url),
        db_type=0, # TODO
        )
    t = hive_metadata.tables['node_metadata']
    t.insert().execute(
        id=42,
        partition_dimension_id=13,
        name='node42',
        uri=str(p42_metadata.bind.url),
        )

    got = connect.get_engine(hive_metadata, 'frob', 123)
    assert isinstance(got, sq.engine.Engine)
    eq_(str(got.url), str(p42_metadata.bind.url))

def test_bad_dimension():
    tmp = maketemp()

    hive_metadata = sq.MetaData()
    hive_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'hive.db'),
        strategy='threadlocal',
        )
    for table in hive.metadata.tables.values():
        table.tometadata(hive_metadata)
    hive_metadata.create_all()

    t = hive_metadata.tables['partition_dimension_metadata']
    t.insert().execute(
        id=13,
        name='these-are-not-the-droids',
        index_uri='fake',
        db_type=0, # TODO
        )

    e = assert_raises(
        connect.NoSuchDimensionError,
        connect.get_engine,
        hive_metadata,
        'frob',
        123,
        )
    eq_(
        str(e),
        'No such dimension: %r' % 'frob',
        )

def test_bad_id():
    tmp = maketemp()

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'directory.db'),
        strategy='threadlocal',
        )
    pri = directory.dynamic_table(
        table=directory.hive_primary,
        metadata=directory_metadata,
        name='hive_primary_frob',
        )
    pri.create()
    pri.insert().execute(
        # not the right id
        id=9999,
        node=42,
        secondary_index_count=0,
        last_updated=datetime.datetime.now(),
        read_only=False,
        )

    hive_metadata = sq.MetaData()
    hive_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'hive.db'),
        strategy='threadlocal',
        )
    for table in hive.metadata.tables.values():
        table.tometadata(hive_metadata)
    hive_metadata.create_all()

    t = hive_metadata.tables['partition_dimension_metadata']
    t.insert().execute(
        id=13,
        name='frob',
        index_uri=str(directory_metadata.bind.url),
        db_type=0, # TODO
        )

    got = connect.get_engine(hive_metadata, 'frob', 123)
    assert got is None

def test_bad_node():
    tmp = maketemp()

    directory_metadata = sq.MetaData()
    directory_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'directory.db'),
        strategy='threadlocal',
        )
    pri = directory.dynamic_table(
        table=directory.hive_primary,
        metadata=directory_metadata,
        name='hive_primary_frob',
        )
    pri.create()
    pri.insert().execute(
        id=123,
        node=42,
        secondary_index_count=0,
        last_updated=datetime.datetime.now(),
        read_only=False,
        )

    hive_metadata = sq.MetaData()
    hive_metadata.bind = sq.create_engine(
        'sqlite:///%s' % os.path.join(tmp, 'hive.db'),
        strategy='threadlocal',
        )
    for table in hive.metadata.tables.values():
        table.tometadata(hive_metadata)
    hive_metadata.create_all()

    t = hive_metadata.tables['partition_dimension_metadata']
    t.insert().execute(
        id=13,
        name='frob',
        index_uri=str(directory_metadata.bind.url),
        db_type=0, # TODO
        )
    t = hive_metadata.tables['node_metadata']
    t.insert().execute(
        # not the right id
        id=34,
        partition_dimension_id=13,
        name='node34',
        uri='fake',
        )

    e = assert_raises(
        connect.NoSuchNodeError,
        connect.get_engine,
        hive_metadata,
        'frob',
        123,
        )
    eq_(
        str(e),
        'No such node: dimension %r, node_id 42' % 'frob',
        )