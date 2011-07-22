fakeredis: A fake version of a redis-py
=======================================

fakeredis is a pure python implementation of the redis-py python client
that simulates talking to a redis server.  This was created for a single
purpose: **to write unittests**.  Setting up redis is not hard, but
I do believe that a unittest (you know, those tests that you constantly
run after every little change) should not talk to an external server
(such as redis).  As a result, most of my preexisting test code either
mocks out the relevent portions of talking to redis or implements a
subset of a fakeredis.

How to Use
==========

The intent is for fakeredis to act as though you're talking to a real
redis server.  It does this by storing state in the redis client itself.
For example::

  >>> import fakeredis
  >>> r = fakeredis.FakeRedis()
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

Supported Commands
==================

All of the hashes, lists, sets, and sorted sets commands are implemented
in fakeredis.  There's also support for the ``keys, set, get`` commands.

Adding New Commands
===================

Adding support for more redis commands is easy:

* Add unittests for the new command.
* Implement new command.

To ensure parity with the real redis, there are a set of integration tests
that mirror the unittests.  For every unittest that is written, the same
test is run against a real redis instance using a real redis-py client
instance.  In order to run these tests you must have a redis server running
on localhost, port 6379 (the default settings).  The integration tests use
db=10 in order to minimize collisions with an existing redis instance.
