import re


def _normalise_path(p):
    if not p.startswith("/"):
        return "/" + p
    else:
        return p


def _build_predicate_regex(pats):
    regexes = [re.compile(regex) for regex in pats]

    def predicate(s):
        return any(regex.match(s) for regex in regexes)

    return predicate


def always_true(_):
    return True


class TestRegister:
    def __init__(self):
        self._tests = {}

    def register(self, path, callback):
        path = _normalise_path(path)
        self._tests[path] = callback

    def register_batch(self, prefix, pairs):
        # ensure a trailing slash
        prefix = str(prefix)
        if not prefix.endswith("/"):
            prefix += "/"

        for path, callback in pairs:
            self.register(prefix + path.lstrip("/"), callback)

    def paths(self, patterns=None):
        if patterns:
            pred = _build_predicate_regex(patterns)
        else:
            pred = always_true

        # return filter(pred, self._tests.keys())
        return filter(pred, self._tests.keys())

    def run(self, path, fix):
        self._tests[path](fix)
