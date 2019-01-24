from __future__ import print_function, division, absolute_import
import operator
import functools

import hypothesis
import hypothesis.stateful
import hypothesis.strategies as st
from nose.tools import assert_equal
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

import redis
import fakeredis


self_strategy = st.runner()


@st.composite
def sample_attr(draw, name):
    """Strategy for sampling a specific attribute from a state machine"""
    machine = draw(self_strategy)
    values = getattr(machine, name)
    position = draw(st.integers(min_value=0, max_value=len(values) - 1))
    return values[position]


keys = sample_attr('keys')
fields = sample_attr('fields')
values = sample_attr('values')
scores = sample_attr('scores')

int_as_bytes = st.builds(lambda x: str(x).encode(), st.integers())
float_as_bytes = st.builds(lambda x: repr(x).encode(), st.floats(width=32))
counts = st.integers(min_value=-3, max_value=3) | st.integers()
limits = st.just(()) | st.tuples(st.just('limit'), counts, counts)
# Redis has an integer overflow bug in swapdb, so we confine the numbers to
# a limited range (https://github.com/antirez/redis/issues/5737).
dbnums = st.integers(min_value=0, max_value=3) | st.integers(min_value=-1000, max_value=1000)
# The filter is to work around https://github.com/antirez/redis/issues/5632
patterns = (st.text(alphabet=st.sampled_from('[]^$*.?-azAZ\\\r\n\t'))
            | st.binary().filter(lambda x: b'\0' not in x))
score_tests = scores | st.builds(lambda x: b'(' + repr(x).encode(), scores)
string_tests = (
    st.sampled_from([b'+', b'-'])
    | st.builds(operator.add, st.sampled_from([b'(', b'[']), fields))
# Redis has integer overflow bugs in time computations, which is why we set a maximum.
expires_seconds = st.integers(min_value=100000, max_value=10000000000)
expires_ms = st.integers(min_value=100000000, max_value=10000000000000)


class WrappedException(object):
    """Wraps an exception for the purposes of comparison."""
    def __init__(self, exc):
        self.wrapped = exc

    def __str__(self):
        return str(self.wrapped)

    def __repr__(self):
        return 'WrappedException({0!r})'.format(self.wrapped)

    def __eq__(self, other):
        if not isinstance(other, WrappedException):
            return NotImplemented
        if type(self.wrapped) != type(other.wrapped):    # noqa: E721
            return False
        # TODO: re-enable after more carefully handling order of error checks
        # return self.wrapped.args == other.wrapped.args
        return True

    def __ne__(self, other):
        if not isinstance(other, WrappedException):
            return NotImplemented
        return not self == other


def wrap_exceptions(obj):
    if isinstance(obj, list):
        return [wrap_exceptions(item) for item in obj]
    elif isinstance(obj, Exception):
        return WrappedException(obj)
    else:
        return obj


def sort_list(lst):
    if isinstance(lst, list):
        return sorted(lst)
    else:
        return lst


def flatten(args):
    if isinstance(args, (list, tuple)):
        for arg in args:
            for item in flatten(arg):
                yield item
    elif args is not None:
        yield args


def default_normalize(x):
    return x


class Command(object):
    def __init__(self, *args):
        self.args = tuple(flatten(args))

    def __repr__(self):
        parts = [repr(arg) for arg in self.args]
        return 'Command({})'.format(', '.join(parts))

    @staticmethod
    def encode(arg):
        encoder = redis.connection.Encoder('utf-8', 'replace', False)
        return encoder.encode(arg)

    @property
    def normalize(self):
        command = self.encode(self.args[0]).lower() if self.args else None
        # Functions that return a list in arbitrary order
        unordered = {
            b'keys',
            b'sort',
            b'hgetall', b'hkeys', b'hvals',
            b'sdiff', b'sinter', b'sunion',
            b'smembers'
        }
        if command in unordered:
            return sort_list
        else:
            return lambda x: x

    @property
    def testable(self):
        """Whether this command is suitable for a test.

        The fuzzer can create commands with behaviour that is
        non-deterministic, not supported, or which hits redis bugs.
        """
        N = len(self.args)
        if N == 0:
            return False
        command = self.encode(self.args[0]).lower()
        if command == b'keys' and N == 2 and self.args[1] != b'*':
            return False
        return True


def commands(*args, **kwargs):
    return st.builds(functools.partial(Command, **kwargs), *args)


# TODO: all expiry-related commands
common_commands = (
    commands(st.sampled_from(['del', 'persist', 'type']), keys)
    | commands(st.just('exists'), st.lists(keys))
    | commands(st.just('keys'), st.just('*'))
    # Disabled for now due to redis giving wrong answers
    # (https://github.com/antirez/redis/issues/5632)
    # | st.tuples(st.just('keys'), patterns)
    | commands(st.just('move'), keys, dbnums)
    | commands(st.sampled_from(['rename', 'renamenx']), keys, keys)
    # TODO: find a better solution to sort instability than throwing
    # away the sort entirely with normalize. This also prevents us
    # using LIMIT.
    | commands(st.just('sort'), keys,
               st.none() | st.just('asc'),
               st.none() | st.just('desc'),
               st.none() | st.just('alpha'))
)

# TODO: tests for select
connection_commands = (
    commands(st.just('echo'), values)
    | commands(st.just('ping'), st.lists(values, max_size=2))
    | commands(st.just('swapdb'), dbnums, dbnums)
)

string_create_commands = commands(st.just('set'), keys, values)
string_commands = (
    commands(st.just('append'), keys, values)
    | commands(st.just('bitcount'), keys)
    | commands(st.just('bitcount'), keys, values, values)
    | commands(st.sampled_from(['incr', 'decr']), keys)
    | commands(st.sampled_from(['incrby', 'decrby']), keys, values)
    # Disabled for now because Python can't exactly model the long doubles.
    # TODO: make a more targeted test that checks the basics.
    # TODO: check how it gets stringified, without relying on hypothesis
    # to get generate a get call before it gets overwritten.
    # | commands(st.just('incrbyfloat'), keys, st.floats(width=32))
    | commands(st.just('get'), keys)
    | commands(st.just('getbit'), keys, counts)
    | commands(st.just('setbit'), keys, counts,
               st.integers(min_value=0, max_value=1) | st.integers())
    | commands(st.sampled_from(['substr', 'getrange']), keys, counts, counts)
    | commands(st.just('getset'), keys, values)
    | commands(st.just('mget'), st.lists(keys))
    | commands(st.sampled_from(['mset', 'msetnx']), st.lists(st.tuples(keys, values)))
    | commands(st.just('set'), keys, values,
               st.none() | st.just('nx'), st.none() | st.just('xx'))
    | commands(st.just('setex'), keys, expires_seconds, values)
    | commands(st.just('psetex'), keys, expires_ms, values)
    | commands(st.just('setnx'), keys, values)
    | commands(st.just('setrange'), keys, counts, values)
    | commands(st.just('strlen'), keys)
)

# TODO: add a test for hincrbyfloat. See incrbyfloat for why this is
# problematic.
hash_create_commands = (
    commands(st.just('hmset'), keys, st.lists(st.tuples(fields, values), min_size=1))
)
hash_commands = (
    commands(st.just('hmset'), keys, st.lists(st.tuples(fields, values)))
    | commands(st.just('hdel'), keys, st.lists(fields))
    | commands(st.just('hexists'), keys, fields)
    | commands(st.just('hget'), keys, fields)
    | commands(st.sampled_from(['hgetall', 'hkeys', 'hvals']), keys)
    | commands(st.just('hincrby'), keys, fields, st.integers())
    | commands(st.just('hlen'), keys)
    | commands(st.just('hmget'), keys, st.lists(fields))
    | commands(st.sampled_from(['hset', 'hmset']), keys, st.lists(st.tuples(fields, values)))
    | commands(st.just('hsetnx'), keys, fields, values)
    | commands(st.just('hstrlen'), keys, fields)
)

# TODO: blocking commands
list_create_commands = commands(st.just('rpush'), keys, st.lists(values, min_size=1))
list_commands = (
    commands(st.just('lindex'), keys, counts)
    | commands(st.just('linsert'), keys,
               st.sampled_from(['before', 'after', 'BEFORE', 'AFTER']) | st.binary(),
               values, values)
    | commands(st.just('llen'), keys)
    | commands(st.sampled_from(['lpop', 'rpop']), keys)
    | commands(st.sampled_from(['lpush', 'lpushx', 'rpush', 'rpushx']), keys, st.lists(values))
    | commands(st.just('lrange'), keys, counts, counts)
    | commands(st.just('lrem'), keys, counts, values)
    | commands(st.just('lset'), keys, counts, values)
    | commands(st.just('ltrim'), keys, counts, counts)
    | commands(st.just('rpoplpush'), keys, keys)
)

# TODO:
# - find a way to test srandmember, spop which are random
# - sscan
set_create_commands = (
    commands(st.just('sadd'), keys, st.lists(fields, min_size=1))
)
set_commands = (
    commands(st.just('sadd'), keys, st.lists(fields,))
    | commands(st.just('scard'), keys)
    | commands(st.sampled_from(['sdiff', 'sinter', 'sunion']), st.lists(keys))
    | commands(st.sampled_from(['sdiffstore', 'sinterstore', 'sunionstore']),
               keys, st.lists(keys))
    | commands(st.just('sismember'), keys, fields)
    | commands(st.just('smembers'), keys)
    | commands(st.just('smove'), keys, keys, fields)
    | commands(st.just('srem'), keys, st.lists(fields))
)


def build_zstore(command, dest, sources, weights, aggregate):
    args = [command, dest, len(sources)]
    args += [source[0] for source in sources]
    if weights:
        args.append('weights')
        args += [source[1] for source in sources]
    if aggregate:
        args += ['aggregate', aggregate]
    return Command(args)


# TODO: zscan, zpopmin/zpopmax, bzpopmin/bzpopmax, probably more
zset_create_commands = (
    commands(st.just('zadd'), keys, st.lists(st.tuples(scores, fields), min_size=1))
)
zset_commands = (
    # TODO: test xx, nx, ch, incr
    commands(st.just('zadd'), keys, st.lists(st.tuples(scores, fields)))
    | commands(st.just('zcard'), keys)
    | commands(st.just('zcount'), keys, score_tests, score_tests)
    | commands(st.just('zincrby'), keys, scores, fields)
    | commands(st.sampled_from(['zrange', 'zrevrange']), keys, counts, counts,
               st.none() | st.just('withscores'))
    | commands(st.sampled_from(['zrangebyscore', 'zrevrangebyscore']),
               keys, score_tests, score_tests,
               limits,
               st.none() | st.just('withscores'))
    | commands(st.sampled_from(['zrank', 'zrevrank']), keys, fields)
    | commands(st.just('zrem'), keys, st.lists(fields))
    | commands(st.just('zremrangebyrank'), keys, counts, counts)
    | commands(st.just('zremrangebyscore'), keys, score_tests, score_tests)
    | commands(st.just('zscore'), keys, fields)
    | st.builds(build_zstore,
                command=st.sampled_from(['zunionstore', 'zinterstore']),
                dest=keys, sources=st.lists(st.tuples(keys, float_as_bytes)),
                weights=st.booleans(),
                aggregate=st.sampled_from([None, 'sum', 'min', 'max']))
)

zset_no_score_create_commands = (
    commands(st.just('zadd'), keys, st.lists(st.tuples(st.just(0), fields), min_size=1))
)
zset_no_score_commands = (
    # TODO: test xx, nx, ch, incr
    commands(st.just('zadd'), keys, st.lists(st.tuples(st.just(0), fields)))
    | commands(st.just('zlexcount'), keys, string_tests, string_tests)
    | commands(st.sampled_from(['zrangebylex', 'zrevrangebylex']),
               keys, string_tests, string_tests,
               limits)
    | commands(st.just('zremrangebylex'), keys, string_tests, string_tests)
)

transaction_commands = (
    commands(st.sampled_from(['multi', 'discard', 'exec', 'unwatch']))
    | commands(st.just('watch'), keys)
)

server_commands = (
    # TODO: real redis raises an error if there is a save already in progress.
    # Find a better way to test this.
    # commands(st.just('bgsave'))
    commands(st.just('dbsize'))
    | commands(st.sampled_from(['flushdb', 'flushall']), st.sampled_from([[], 'async']))
    # TODO: result is non-deterministic
    # | commands(st.just('lastsave'))
    | commands(st.just('save'))
)

bad_commands = (
    # redis-py splits the command on spaces, and hangs if that ends up
    # being an empty list
    commands(st.text().filter(lambda x: bool(x.split())),
             st.lists(st.binary() | st.text()))
)


@hypothesis.settings(max_examples=1000, timeout=hypothesis.unlimited)
class CommonMachine(hypothesis.stateful.GenericStateMachine):
    create_command_strategy = None

    STATE_EMPTY = 0
    STATE_INIT = 1
    STATE_RUNNING = 2

    def __init__(self):
        super(CommonMachine, self).__init__()
        self.fake = fakeredis.FakeStrictRedis()
        try:
            self.real = redis.StrictRedis('localhost', port=6379)
            self.real.ping()
        except redis.ConnectionError:
            raise SkipTest('redis is not running')
        self.transaction_normalize = []
        self.keys = []
        self.fields = []
        self.values = []
        self.scores = []
        self.state = self.STATE_EMPTY
        try:
            self.real.execute_command('discard')
        except redis.ResponseError:
            pass
        self.real.flushall()

    def teardown(self):
        self.real.connection_pool.disconnect()
        self.fake.connection_pool.disconnect()
        super(CommonMachine, self).teardown()

    def _evaluate(self, client, command):
        try:
            result = client.execute_command(*command.args)
            if result != 'QUEUED':
                result = command.normalize(result)
            exc = None
        except Exception as e:
            result = exc = e
        return wrap_exceptions(result), exc

    def _compare(self, command):
        fake_result, fake_exc = self._evaluate(self.fake, command)
        real_result, real_exc = self._evaluate(self.real, command)

        if fake_exc is not None and real_exc is None:
            raise fake_exc
        elif real_exc is not None and fake_exc is None:
            assert_equal(real_exc, fake_exc, "Expected exception {0} not raised".format(real_exc))
        elif (real_exc is None and isinstance(real_result, list)
              and command.args and command.args[0].lower() == 'exec'):
            # Transactions need to use the normalize functions of the
            # component commands.
            assert_equal(len(self.transaction_normalize), len(real_result))
            assert_equal(len(self.transaction_normalize), len(fake_result))
            for n, r, f in zip(self.transaction_normalize, real_result, fake_result):
                assert_equal(n(f), n(r))
            self.transaction_normalize = []
        elif real_exc is None and command.args and command.args[0].lower() == 'discard':
            self.transaction_normalize = []
        else:
            assert_equal(fake_result, real_result)
            if real_result == b'QUEUED':
                # Since redis removes the distinction between simple strings and
                # bulk strings, this might not actually indicate that we're in a
                # transaction. But it is extremely unlikely that hypothesis will
                # find such examples.
                self.transaction_normalize.append(command.normalize)

    def _init_attrs(self, attrs):
        for key, value in attrs.items():
            setattr(self, key, value)

    def _init_data(self, init_commands):
        for command in init_commands:
            self._compare(command)

    def steps(self):
        if self.state == self.STATE_EMPTY:
            attrs = {
                'keys': st.lists(st.binary(), min_size=2, max_size=5, unique=True),
                'fields': st.lists(st.binary(), min_size=2, max_size=5, unique=True),
                'values': st.lists(st.binary() | int_as_bytes | float_as_bytes,
                                   min_size=2, max_size=5, unique=True),
                'scores': st.lists(st.floats(width=32), min_size=2, max_size=5, unique=True)
            }
            return st.fixed_dictionaries(attrs)
        elif self.state == self.STATE_INIT:
            return st.lists(self.create_command_strategy)
        else:
            return self.command_strategy

    def execute_step(self, step):
        if self.state == self.STATE_EMPTY:
            self._init_attrs(step)
            self.state = self.STATE_INIT if self.create_command_strategy else self.STATE_RUNNING
        elif self.state == self.STATE_INIT:
            self._init_data(step)
            self.state = self.STATE_RUNNING
        else:
            self._compare(step)


class BaseTest(object):
    create_command_strategy = None

    """Base class for test classes."""
    @attr('slow')
    def test(self):
        class Machine(CommonMachine):
            create_command_strategy = self.create_command_strategy
            command_strategy = self.command_strategy

        hypothesis.stateful.run_state_machine_as_test(Machine)


class TestConnection(BaseTest):
    command_strategy = connection_commands | common_commands


class TestString(BaseTest):
    create_command_strategy = string_create_commands
    command_strategy = string_commands | common_commands


class TestHash(BaseTest):
    create_command_strategy = hash_create_commands
    command_strategy = hash_commands | common_commands


class TestList(BaseTest):
    create_command_strategy = list_create_commands
    command_strategy = list_commands | common_commands


class TestSet(BaseTest):
    create_command_strategy = set_create_commands
    command_strategy = set_commands | common_commands


class TestZSet(BaseTest):
    create_command_strategy = zset_create_commands
    command_strategy = zset_commands | common_commands


class TestZSetNoScores(BaseTest):
    create_command_strategy = zset_no_score_create_commands
    command_strategy = zset_no_score_commands | common_commands


class TestTransaction(BaseTest):
    create_command_strategy = string_create_commands
    command_strategy = transaction_commands | string_commands | common_commands


class TestServer(BaseTest):
    create_command_strategy = string_create_commands
    command_strategy = server_commands | string_commands | common_commands


class TestJoint(BaseTest):
    create_command_strategy = (
        string_create_commands | hash_create_commands | list_create_commands
        | set_create_commands | zset_create_commands)
    command_strategy = (
        transaction_commands | server_commands | connection_commands
        | string_commands | hash_commands | list_commands | set_commands
        | zset_commands | common_commands | bad_commands)


@st.composite
def delete_arg(draw, commands):
    command = draw(commands)
    if command.args:
        pos = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        command.args = command.args[:pos] + command.args[pos + 1:]
    return command


@st.composite
def command_args(draw, commands):
    """Generate an argument from some command"""
    command = draw(commands)
    hypothesis.assume(len(command.args))
    return draw(st.sampled_from(command.args))


def mutate_arg(draw, commands, mutate):
    command = draw(commands)
    if command.args:
        pos = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        arg = mutate(Command.encode(command.args[pos]))
        command.args = command.args[:pos] + (arg,) + command.args[pos + 1:]
    return command


@st.composite
def replace_arg(draw, commands, replacements):
    return mutate_arg(draw, commands, lambda arg: draw(replacements))


@st.composite
def uppercase_arg(draw, commands):
    return mutate_arg(draw, commands, lambda arg: arg.upper())


@st.composite
def prefix_arg(draw, commands, prefixes):
    return mutate_arg(draw, commands, lambda arg: draw(prefixes) + arg)


@st.composite
def suffix_arg(draw, commands, suffixes):
    return mutate_arg(draw, commands, lambda arg: arg + draw(suffixes))


@st.composite
def add_arg(draw, commands, arguments):
    command = draw(commands)
    arg = draw(arguments)
    pos = draw(st.integers(min_value=0, max_value=len(command.args)))
    command.args = command.args[:pos] + (arg,) + command.args[pos:]
    return command


@st.composite
def swap_args(draw, commands):
    command = draw(commands)
    if len(command.args) >= 2:
        pos1 = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        pos2 = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        hypothesis.assume(pos1 != pos2)
        args = list(command.args)
        arg1 = args[pos1]
        arg2 = args[pos2]
        args[pos1] = arg2
        args[pos2] = arg1
        command.args = tuple(args)
    return command


def mutated_commands(commands):
    args = st.sampled_from([b'withscores', b'xx', b'nx', b'ex', b'px', b'weights', b'aggregate',
                            b'', b'0', b'-1', b'nan', b'inf', b'-inf']) | command_args(commands)
    affixes = st.sampled_from([b'\0', b'-', b'+', b'\t', b'\n', b'0000']) | st.binary()
    return st.recursive(
        commands,
        lambda x:
            delete_arg(x)
            | replace_arg(x, args)
            | uppercase_arg(x)
            | prefix_arg(x, affixes)
            | suffix_arg(x, affixes)
            | add_arg(x, args)
            | swap_args(x))


class TestFuzz(BaseTest):
    command_strategy = mutated_commands(TestJoint.command_strategy)
    command_strategy = command_strategy.filter(lambda command: command.testable)
