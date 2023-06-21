import os
import re
import shutil
import dmtest.fixture as fixture
import dmtest.process as process
import dmtest.dependency_tracker as dep

from typing import NamedTuple, Callable, Optional


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

    def register_batch(self, prefix, tests, batch_dep_fn=None):
        # ensure a trailing slash
        prefix = str(prefix)
        if not prefix.endswith("/"):
            prefix += "/"

        for test in tests:
            if len(test) == 2:
                path, callback = test
                dep_fn = batch_dep_fn
            else:
                path, callback, dep_fn = test
            self.register(prefix + path.lstrip("/"), callback, dep_fn)

    def paths(self, results, result_set, filt=None):
        selected = []

        for t in self._tests.keys():
            res_list = results.get_test_results(t, result_set)
            if filt.matches(t, res_list):
                selected.append(t)

        return selected

    def check_deps(self, deps: dep.DepTracker):
        for target in deps.targets:
            if not has_target(target):
                raise MissingTestDep(f"{target} target")
        for exe in deps.executables:
            if shutil.which(exe) is None:
                raise MissingTestDep(f"{exe} executable")

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


def has_repo(path: str) -> bool:
    return os.path.isdir(os.path.join(path, ".git"))


def check_linux_repo():
    path = os.getenv("DMTEST_KERNEL_SOURCE", "linux")
    if not has_repo(path):
        raise MissingTestDep(f"{path} repository")
