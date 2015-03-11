fakeredis: A fake version of a redis-py
=======================================

.. image:: https://secure.travis-ci.org/jamesls/fakeredis.png?branch=master
   :target: http://travis-ci.org/jamesls/fakeredis

.. image:: https://pypip.in/version/fakeredis/badge.svg
   :target: https://pypi.python.org/pypi/fakeredis

.. image:: https://pypip.in/py_versions/fakeredis/badge.svg
    :target: https://pypi.python.org/pypi/fakeredis/

.. image:: https://pypip.in/license/fakeredis/badge.svg
    :target: https://pypi.python.org/pypi/fakeredis/

.. image:: https://coveralls.io/repos/jamesls/fakeredis/badge.png?branch=master
   :target: https://coveralls.io/r/jamesls/fakeredis


fakeredis is a pure python implementation of the redis-py python client
that simulates talking to a redis server.  This was created for a single
purpose: **to write unittests**.  Setting up redis is not hard, but
many times you want to write unittests that do not talk to an external server
(such as redis).  This module now allows tests to simply use this
module as a reasonable substitute for redis.


How to Use
==========

The intent is for fakeredis to act as though you're talking to a real
redis server.  It does this by storing state in the fakeredis module.
For example::

  >>> import fakeredis
  >>> r = fakeredis.FakeStrictRedis()
  >>> r.set('foo', 'bar')
  True
  >>> r.get('foo')
  'bar'
  >>> r.lpush('bar', 1)
  1
  >>> r.lpush('bar', 2)
  2
  >>> r.lrange('bar', 0, -1)
  [2, 1]

By storing state in the fakeredis module, instances can share
data::

  >>> import fakeredis
  >>> r1 = fakeredis.FakeStrictRedis()
  >>> r1.set('foo', 'bar')
  True
  >>> r2 = fakeredis.FakeStrictRedis()
  >>> r2.get('foo')
  'bar'
  >>> r2.set('bar', 'baz')
  True
  >>> r1.get('bar')
  'baz'
  >>> r2.get('bar')
  'baz'


Fakeredis implements the same interface as `redis-py`_, the
popular redis client for python, and models the responses
of redis 2.6.


Unimplemented Commands
======================

All of the redis commands are implemented in fakeredis with
these exceptions:


set
---

 * sscan


hash
----

 * hscan


hyperloglog
-----------

 * pfcount
 * pfadd
 * pfmerge


generic
-------

 * restore
 * dump
 * pexpireat
 * pexpire
 * migrate
 * object


server
------

 * client list
 * lastsave
 * slowlog
 * debug object
 * shutdown
 * debug segfault
 * command count
 * monitor
 * client kill
 * cluster slots
 * role
 * config resetstat
 * time
 * config get
 * config set
 * save
 * client setname
 * command getkeys
 * config rewrite
 * sync
 * client getname
 * bgrewriteaof
 * slaveof
 * info
 * client pause
 * bgsave
 * command
 * dbsize
 * command info


sorted_set
----------

 * zrangebylex
 * zscan
 * zrevrangebylex
 * zremrangebylex
 * zlexcount


connection
----------

 * echo
 * select
 * quit
 * auth


scripting
---------

 * script flush
 * script kill
 * script load
 * evalsha
 * eval
 * script exists


string
------

 * incrbyfloat
 * bitop
 * bitpos


pubsub
------

 * punsubscribe
 * subscribe
 * publish
 * pubsub
 * psubscribe
 * unsubscribe


Contributing
============

Contributions are welcome.  Please see the `contributing guide`_ for
more details.


Running the Tests
=================

To ensure parity with the real redis, there are a set of integration tests
that mirror the unittests.  For every unittest that is written, the same
test is run against a real redis instance using a real redis-py client
instance.  In order to run these tests you must have a redis server running
on localhost, port 6379 (the default settings).  The integration tests use
db=10 in order to minimize collisions with an existing redis instance.


To run all the tests, install the requirements file::

    pip install -r requirements.txt

If you just want to run the unittests::

    nosetests test_fakeredis.py:TestFakeStrictRedis test_fakeredis.py:TestFakeRedis

Because this module is attempting to provide the same interface as `redis-py`_,
the python bindings to redis, a reasonable way to test this to to take each
unittest and run it against a real redis server.  fakeredis and the real redis
server should give the same result.  This ensures parity between the two.  You
can run these "integration" tests like this::

    nosetests test_fakeredis.py:TestRealStrictRedis test_fakeredis.py:TestRealRedis

In terms of implementation, ``TestRealRedis`` is a subclass of
``TestFakeRedis`` that overrides a factory method to create
an instance of ``redis.Redis`` (an actual python client for redis)
instead of ``fakeredis.FakeStrictRedis``.

To run both the unittests and the "integration" tests, run::

    nosetests

If redis is not running and you try to run tests against a real redis server,
these tests will have a result of 'S' for skipped.

There are some tests that test redis blocking operations that are somewhat
slow.  If you want to skip these tests during day to day development,
they have all been tagged as 'slow' so you can skip them by running::

    nosetests -a '!slow'


.. _redis-py: http://redis-py.readthedocs.org/en/latest/index.html
.. _contributing guide: https://github.com/jamesls/fakeredis/blob/master/CONTRIBUTING.rst
