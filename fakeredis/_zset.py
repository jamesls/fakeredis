import sortedcontainers


class ZSet(object):
    def __init__(self):
        self._bylex = sortedcontainers.SortedDict()
        self._byscore = sortedcontainers.SortedSet()

    def __contains__(self, value):
        return value in self._bylex

    def __setitem__(self, value, score):
        try:
            old_score = self._bylex[value]
        except KeyError:
            pass
        else:
            self._byscore.discard((old_score, value))
        self._bylex[value] = score
        self._byscore.add((score, value))

    def __getitem__(self, key):
        return self._bylex[key]

    def get(self, key, default=None):
        return self._bylex.get(key, default)

    def __len__(self):
        return len(self._bylex)

    def __iter__(self):
        def gen():
            for score, value in self._byscore:
                yield value

        return gen()

    def discard(self, key):
        try:
            score = self._bylex.pop(key)
        except KeyError:
            return
        else:
            self._byscore.remove((score, key))

    def zcount(self, min_, max_):
        pos1 = self._byscore.bisect_left(min_)
        pos2 = self._byscore.bisect_left(max_)
        return max(0, pos2 - pos1)

    def zlexcount(self, min_value, min_exclusive, max_value, max_exclusive):
        if min_exclusive:
            pos1 = self._bylex.bisect_right(min_value)
        else:
            pos1 = self._bylex.bisect_left(min_value)
        if max_exclusive:
            pos2 = self._bylex.bisect_left(max_value)
        else:
            pos2 = self._bylex.bisect_right(max_value)
        return max(0, pos2 - pos1)

    def islice_score(self, start, stop, reverse=False):
        return self._byscore.islice(start, stop, reverse)

    def irange_lex(self, start, stop, inclusive=(True, True), reverse=False):
        return self._bylex.irange(start, stop, inclusive=inclusive, reverse=reverse)

    def irange_score(self, start, stop, reverse=False):
        return self._byscore.irange(start, stop, reverse=reverse)

    def rank(self, member):
        return self._byscore.index((self._bylex[member], member))

    def items(self):
        return self._bylex.items()
