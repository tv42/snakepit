import errno
import os
import shutil
import sys

def mkdir(*a, **kw):
    try:
        os.mkdir(*a, **kw)
    except OSError, e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

def maketemp():
    tmp = os.path.join(os.path.dirname(__file__), 'tmp')
    mkdir(tmp)

    caller = sys._getframe(1)

    # kludgy way to detect methods, if their first arg is called
    # "self"
    if (caller.f_code.co_varnames
        and caller.f_code.co_varnames[0] == 'self'):
        its_self = caller.f_locals['self']
        name = '%s.%s.%s' % (
            its_self.__class__.__module__,
            its_self.__class__.__name__,
            caller.f_code.co_name,
            )
    else:
        name = '%s.%s' % (
            sys._getframe(1).f_globals['__name__'],
            caller.f_code.co_name,
            )

    tmp = os.path.join(tmp, name)
    try:
        shutil.rmtree(tmp)
    except OSError, e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise
    os.mkdir(tmp)
    return tmp

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass, e:
        return e
    else:
        if hasattr(excClass,'__name__'): excName = excClass.__name__
        else: excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

