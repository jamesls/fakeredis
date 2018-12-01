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

    def __len__(self):
        return len(self._bylex)
