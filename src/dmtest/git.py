import dmtest.fs as fs
import dmtest.process as process
import dmtest.utils as utils

import os
import sys
from pathlib import Path
from contextlib import contextmanager

# --------------------------------
kernel_source = os.getenv("DMTEST_KERNEL_SOURCE", "../linux")


class Git:
    def __init__(self, origin):
        if not Path(origin, ".git").exists():
            raise ValueError("not a git directory")
        self.origin = origin

    @classmethod
    def clone(cls, origin, dir):
        process.run(f"git clone {origin} {dir}")
        return cls(dir)

    def checkout(self, tag):
        process.run(f"git -C {self.origin} checkout {tag}")

    def delete(self):
        process.run(f"rm -rf {self.origin}")


# --------------------------------

TAGS = [
    f"v{major}.{minor}"
    for major, minor_ranges in [("2.6", range(12, 40)), ("3", range(0, 3))]
    for minor in minor_ranges
]


def drop_caches():
    process.run("echo 3 > /proc/sys/vm/drop_caches")


def prepare_(dev, fs_type, format_opts=None):
    if format_opts is None:
        format_opts = {}

    linux_fs = fs_type(dev)
    linux_fs.format(**format_opts)

    with linux_fs.mount_and_chdir("./kernel_builds", discard=False):
        return Git.clone(kernel_source, "linux")


def prepare(dev, fs_type):
    with utils.timed("git_prepare"):
        prepare_(dev, fs_type)


def prepare_no_discard(dev, fs_type):
    with utils.timed("git_prepare"):
        prepare_(dev, fs_type, discard=False)


def extract(dev, fs_type, tags=None):
    if tags is None:
        tags = TAGS

    linux_fs = fs_type(dev)

    with linux_fs.mount_and_chdir("./kernel_builds", discard=False):
        repo = Git("linux")

        with utils.timed("extract all versions"):
            for tag in tags:
                with utils.timed(f"checking out {tag}"):
                    repo.checkout(tag)
                    process.run("sync")
                    drop_caches()


def extract_each(dev, fs_type, callback, tags=None):
    if tags is None:
        tags = TAGS

    linux_fs = fs_type(dev)

    with linux_fs.mount_and_chdir("./kernel_builds", discard=False):
        repo = Git("linux")

        with utils.timed("extract all versions"):
            for index, tag in enumerate(tags):
                print(f"Checking out {tag} ...", file=sys.stderr)

                with utils.timed(f"checking out {tag}"):
                    repo.checkout(tag)
                    process.run("sync")
                    callback(index)
                    # drop_caches()
