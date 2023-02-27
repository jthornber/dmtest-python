class Table:
    def __init__(self, *targets):
        self._targets = targets

    def __len__(self):
        len(self._targets)

    def __str__(self):
        return ' '.join(self._targets)

    def __iter__(self):
        return iter(self._targets())
