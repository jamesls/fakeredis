fakeredis: A fake version of a redis-py
=======================================

.. image:: https://secure.travis-ci.org/jamesls/fakeredis.svg?branch=master
   :target: http://travis-ci.org/jamesls/fakeredis


.. image:: https://coveralls.io/repos/jamesls/fakeredis/badge.svg?branch=master
   :target: https://coveralls.io/r/jamesls/fakeredis


fakeredis is a pure python implementation of the redis-py python client
that simulates talking to a redis server.  This was created for a single
purpose: **to write unittests**.  Setting up redis is not hard, but
many times you want to write unittests that do not talk to an external server
(such as redis).  This module now allows tests to simply use this
module as a reasonable substitute for redis.

Install
=======

.. code-block:: bash

  $ pip install fakeredis


How to Use
==========

The intent is for fakeredis to act as though you're talking to a real
redis server.  It does this by storing state in the fakeredis module.
For example:

.. code-block:: python

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
data:

.. code-block:: python

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

Because fakeredis stores state at the module level, if you
want to ensure that you have a clean slate for every unit
test you run, be sure to call `r.flushall()` in your
``tearDown`` method.  For example::

    def setUp(self):
        # Setup fake redis for testing.
        self.r = fakeredis.FakeStrictRedis()

    def tearDown(self):
        # Clear data in fakeredis.
        self.r.flushall()


Fakeredis implements the same interface as `redis-py`_, the
popular redis client for python, and models the responses
of redis 2.6.

Unimplemented Commands
======================

All of the redis commands are implemented in fakeredis with
these exceptions:


sorted_set
----------

 * zscan


hash
----

 * hstrlen


string
------

 * bitop
 * bitpos


geo
---

 * geoadd
 * geopos
 * georadius
 * geohash
 * georadiusbymember
 * geodist


generic
-------

 * restore
 * dump
 * migrate
 * object
 * wait


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



cluster
-------

 * cluster getkeysinslot
 * cluster info
 * readwrite
 * cluster slots
 * cluster keyslot
 * cluster addslots
 * readonly
 * cluster saveconfig
 * cluster forget
 * cluster meet
 * cluster slaves
 * cluster nodes
 * cluster countkeysinslot
 * cluster setslot
 * cluster count-failure-reports
 * cluster reset
 * cluster failover
 * cluster set-config-epoch
 * cluster delslots
 * cluster replicate


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


Contributing
============

Contributions are welcome.  Please see the `contributing guide`_ for
more details.

If you'd like to help out, you can start with any of the issues
labeled with `HelpWanted`_.


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
.. _HelpWanted: https://github.com/jamesls/fakeredis/issues?q=is%3Aissue+is%3Aopen+label%3AHelpWanted
