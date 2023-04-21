import toml
from enum import Enum
from pathlib import Path
from typing import Union

from contextlib import contextmanager


class DepTracker:
    def __init__(self):
        self._executables = set()
        self._targets = set()

    def add_executable(self, exe):
        self._executables.add(exe)

    def add_target(self, t):
        self._targets.add(t)

    @property
    def executables(self):
        return sorted(self._executables)

    @property
    def targets(self):
        return sorted(self._targets)


class TestDeps:
    def __init__(self):
        self._deps = {}
        self._updated = False

    def set_deps(self, test_name, exes, targets):
        new_dep = {"executables": exes, "targets": targets}
        if (test_name not in self._deps) or (self._deps[test_name] != new_dep):
            self._updated = True
            self._deps[test_name] = new_dep

    def get_all_executables(self):
        r = set()
        for d in self._deps.values():
            r.update(d["executables"])

        return sorted(r)


def read_test_deps(path):
    deps = TestDeps()
    deps._deps = toml.load(path)
    return deps


def write_test_deps(path, deps):
    if deps._updated:
        with open(path, "w") as f:
            toml.dump(deps._deps, f)


global_dep_tracker = None


@contextmanager
def dep_tracker():
    global global_dep_tracker

    assert not global_dep_tracker
    global_dep_tracker = DepTracker()
    try:
        yield global_dep_tracker
    finally:
        global_dep_tracker = None


def add_exe(name):
    global global_dep_tracker
    if global_dep_tracker:
        global_dep_tracker.add_executable(name)


def add_target(name):
    global global_dep_tracker
    if global_dep_tracker:
        global_dep_tracker.add_target(name)
