import re
import dmtest.fixture as fixture
import dmtest.process as process

from typing import NamedTuple, Callable


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


class MissingTestDep(Exception):
    pass


class Test(NamedTuple):
    dep_fn: Callable[[], None]
    test_fn: Callable[[fixture.Fixture], None]


class TestRegister:
    def __init__(self):
        self._tests = {}

    def register(self, path, callback, dep_fn=None):
        path = _normalise_path(path)
        self._tests[path] = Test(dep_fn, callback)

    def register_batch(self, prefix, pairs, dep_fn=None):
        # ensure a trailing slash
        prefix = str(prefix)
        if not prefix.endswith("/"):
            prefix += "/"

        for path, callback in pairs:
            self.register(prefix + path.lstrip("/"), callback, dep_fn=dep_fn)

    def paths(self, patterns=None):
        if patterns:
            pred = _build_predicate_regex(patterns)
        else:
            pred = always_true

        # return filter(pred, self._tests.keys())
        return filter(pred, self._tests.keys())

    def run(self, path, fix):
        t = self._tests[path]
        if t:
            if t.dep_fn:
                t.dep_fn()
            t.test_fn(fix)
        else:
            raise ValueError(f"can't find test {path}")


def has_target(name):
    def check():
        # It may already be loaded
        (_, stdout, stderr) = process.run(f"dmsetup targets")
        if name in stdout:
            return

        try:
            process.run(f"modprobe dm_{name}")
        except Exception:
            raise MissingTestDep(f"dm_{name} kernel module")

    return check


def has_exe(name):
    def check():
        try:
            process.run(f"which {name}")
        except Exception:
            raise MissingTestDep(f"{name} executable")

    return check
