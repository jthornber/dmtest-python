import re
import shutil
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
        return any(regex.search(s) for regex in regexes)

    return predicate


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

    def paths(self, results, result_set, filt=None):
        selected = []

        for t in self._tests.keys():
            res_list = results.get_test_results(t, result_set)
            if filt.matches(t, res_list):
                selected.append(t)

        return selected

    def run(self, path, fix):
        t = self._tests[path]
        if t:
            if t.dep_fn:
                t.dep_fn()
            t.test_fn(fix)
        else:
            raise ValueError(f"can't find test {path}")


targets_to_kmodules = {
    "thin-pool": "dm_thin_pool",
    "thin": "dm_thin_pool",
    "linear": "device_mapper",
    "bufio_test": "dm_bufio_test",
}


def has_target(target: str) -> bool:
    # It may already be loaded or compiled in
    (_, stdout, stderr) = process.run("dmsetup targets")
    if target in stdout:
        return True

    try:
        kmod = targets_to_kmodules[target]
    except KeyError:
        kmod = f"dm_{target}"

    (code, stdout, stderr) = process.run(f"modprobe {kmod}", raise_on_fail=False)
    return code == 0


def check_target(name: str):
    def check():
        if not has_target(name):
            raise MissingTestDep(f"{name} target")

    return check


def check_exe(name: str):
    def check():
        if shutil.which(name) is None:
            raise MissingTestDep(f"{name} executable")

    return check
