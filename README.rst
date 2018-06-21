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

Alternatively, you can create an instance that does not share data with other
instances, by passing `singleton=False` to the constructor.

Fakeredis implements the same interface as `redis-py`_, the
popular redis client for python, and models the responses
of redis 2.6.

Unimplemented Commands
======================

All of the redis commands are implemented in fakeredis with
these exceptions:


connection
----------

 * auth
 * quit
 * select
 * swapdb


server
------

 * bgrewriteaof
 * bgsave
 * client kill
 * client list
 * client getname
 * client pause
 * client reply
 * client setname
 * command
 * command count
 * command getkeys
 * command info
 * config get
 * config rewrite
 * config set
 * config resetstat
 * dbsize
 * debug object
 * debug segfault
 * info
 * lastsave
 * memory doctor
 * memory help
 * memory malloc-stats
 * memory purge
 * memory stats
 * memory usage
 * monitor
 * role
 * save
 * shutdown
 * slaveof
 * slowlog
 * sync
 * time


string
------

 * bitfield
 * bitop
 * bitpos


cluster
-------

 * cluster addslots
 * cluster count-failure-reports
 * cluster countkeysinslot
 * cluster delslots
 * cluster failover
 * cluster forget
 * cluster getkeysinslot
 * cluster info
 * cluster keyslot
 * cluster meet
 * cluster nodes
 * cluster replicate
 * cluster reset
 * cluster saveconfig
 * cluster set-config-epoch
 * cluster setslot
 * cluster slaves
 * cluster slots
 * readonly
 * readwrite


transactions
------------

 * discard
 * exec
 * multi


generic
-------

 * dump
 * migrate
 * move
 * object
 * randomkey
 * restore
 * touch
 * unlink
 * wait


scripting
---------

 * evalsha
 * script debug
 * script exists
 * script flush
 * script kill
 * script load


geo
---

 * geoadd
 * geohash
 * geopos
 * geodist
 * georadius
 * georadiusbymember


hash
----

 * hstrlen


sorted_set
----------

 * zscan


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


Revision history
================

0.11.0
------
- `#194 <https://github.com/jamesls/fakeredis/pull/194>`_ Support ``score_cast_func`` in zset functions
- `#192 <https://github.com/jamesls/fakeredis/pull/192>`_ Make ``__getitem__`` raise a KeyError for missing keys

0.10.3
------
This is a minor bug-fix release.

- `#189 <https://github.com/jamesls/fakeredis/pull/189>`_ Add 'System' to the list of libc equivalents

0.10.2
------
This is a bug-fix release.

- `#181 <https://github.com/jamesls/fakeredis/issues/181>`_ Upgrade twine & other packaging dependencies
- `#106 <https://github.com/jamesls/fakeredis/issues/106>`_ randomkey method is not implemented, but is not in the list of unimplemented commands
- `#170 <https://github.com/jamesls/fakeredis/pull/170>`_ Prefer readthedocs.io instead of readthedocs.org for doc links
- `#180 <https://github.com/jamesls/fakeredis/issues/180>`_ zadd with no member-score pairs should fail
- `#145 <https://github.com/jamesls/fakeredis/issues/145>`_ expire / _expire: accept 'long' also as time
- `#182 <https://github.com/jamesls/fakeredis/issues/182>`_ Pattern matching does not match redis behaviour
- `#135 <https://github.com/jamesls/fakeredis/issues/135>`_ Scan includes expired keys
- `#185 <https://github.com/jamesls/fakeredis/issues/185>`_ flushall() doesn't clean everything
- `#186 <https://github.com/jamesls/fakeredis/pull/186>`_ Fix psubscribe with handlers
- Run CI on PyPy
- Fix coverage measurement

0.10.1
------
This release merges the fakenewsredis_ fork back into fakeredis. The version
number is chosen to be larger than any fakenewsredis release, so version
numbers between the forks are comparable. All the features listed under
fakenewsredis version numbers below are thus included in fakeredis for the
first time in this release.

Additionally, the following was added:
- `#169 <https://github.com/jamesls/fakeredis/pull/169>`_ Fix set-bit

fakenewsredis 0.10.0
--------------------
- `#14 <https://github.com/ska-sa/fakenewsredis/pull/14>`_ Add option to create an instance with non-shared data
- `#13 <https://github.com/ska-sa/fakenewsredis/pull/13>`_ Improve emulation of redis -> Lua returns
- `#12 <https://github.com/ska-sa/fakenewsredis/pull/12>`_ Update tox.ini: py35/py36 and extras for eval tests
- `#11 <https://github.com/ska-sa/fakenewsredis/pull/11>`_ Fix typo in private method name

fakenewsredis 0.9.5
-------------------
This release makes a start on supporting Lua scripting:
- `#9 <https://github.com/ska-sa/fakenewsredis/pull/9>`_ Add support for StrictRedis.eval for Lua scripts

fakenewsredis 0.9.4
-------------------
This is a minor bugfix and optimization release:
- `#5 <https://github.com/ska-sa/fakenewsredis/issues/5>`_ Update to match redis-py 2.10.6
- `#7 <https://github.com/ska-sa/fakenewsredis/issues/7>`_ Set with invalid expiry time should not set key
- Avoid storing useless expiry times in hashes and sorted sets
- Improve the performance of bulk zadd

fakenewsredis 0.9.3
-------------------
This is a minor bugfix release:
- `#6 <https://github.com/ska-sa/fakenewsredis/pull/6>`_ Fix iteration over pubsub list
- `#3 <https://github.com/ska-sa/fakenewsredis/pull/3>`_ Preserve expiry time when mutating keys
- Fixes to typos and broken links in documentation

fakenewsredis 0.9.2
-------------------
This is the first release of fakenewsredis, based on fakeredis 0.9.0, with the following features and fixes:

- fakeredis `#78 <https://github.com/jamesls/fakeredis/issues/78>`_ Behaviour of transaction() does not match redis-py
- fakeredis `#79 <https://github.com/jamesls/fakeredis/issues/79>`_ Implement redis-py's .lock()
- fakeredis `#90 <https://github.com/jamesls/fakeredis/issues/90>`_ HINCRBYFLOAT changes hash value type to float
- fakeredis `#101 <https://github.com/jamesls/fakeredis/issues/101>`_ Should raise an error when attempting to get a key holding a list)
- fakeredis `#146 <https://github.com/jamesls/fakeredis/issues/146>`_ Pubsub messages and channel names are forced to be ASCII strings on Python 2
- fakeredis `#163 <https://github.com/jamesls/fakeredis/issues/163>`_ getset does not to_bytes the value
- fakeredis `#165 <https://github.com/jamesls/fakeredis/issues/165>`_ linsert implementation is incomplete
- fakeredis `#128 <https://github.com/jamesls/fakeredis/pull/128>`_ Remove `_ex_keys` mapping
- fakeredis `#139 <https://github.com/jamesls/fakeredis/pull/139>`_ Fixed all flake8 errors and added flake8 to Travis CI
- fakeredis `#166 <https://github.com/jamesls/fakeredis/pull/166>`_ Add type checking
- fakeredis `#168 <https://github.com/jamesls/fakeredis/pull/168>`_ Use repr to encode floats in to_bytes

.. _fakenewsredis: https://github.com/ska-sa/fakenewsredis
.. _redis-py: http://redis-py.readthedocs.io/
.. _contributing guide: https://github.com/jamesls/fakeredis/blob/master/CONTRIBUTING.rst
.. _HelpWanted: https://github.com/jamesls/fakeredis/issues?q=is%3Aissue+is%3Aopen+label%3AHelpWanted
