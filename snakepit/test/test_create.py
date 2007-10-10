import nose
from nose.tools import eq_

import os
import sqlalchemy as sq

from snakepit import hive, create

from snakepit.test.util import maketemp

class Create_Hive_Test(object):

    def test_simple(self):
        tmp = maketemp()
        db_uri = 'sqlite:///%s' % os.path.join(tmp, 'hive.db')
        metadata = create.create_hive(db_uri)
        assert isinstance(metadata, sq.MetaData)
        assert metadata.bind is not None
        assert isinstance(metadata.bind, sq.engine.Engine)
        eq_(str(metadata.bind.url), db_uri)
        metadata.bind.dispose()
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
        metadata = create.create_hive(db_uri)
        metadata.bind.dispose()
        metadata = create.create_hive(db_uri)
        metadata.bind.dispose()
