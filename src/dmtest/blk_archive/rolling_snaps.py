from dmtest.assertions import assert_raises, assert_equal
from dmtest.thin.utils import standard_stack, standard_pool
import dmtest.dataset as dataset
import dmtest.device_mapper.dev as dmdev
import dmtest.fs as fs
import dmtest.git as git
import dmtest.pool_stack as ps
import dmtest.process as process
import dmtest.thin.status as status
import dmtest.tvm as tvm
import dmtest.units as units
import dmtest.utils as utils
import dmtest.pattern_stomper as stomper

import os
import threading
import logging as log

# --------------------------------


class BlkArchive:
    def __init__(self, dir, block_size=4096):
        self.dir = os.path.abspath(dir)
        process.run(f"blk-archive create -a {self.dir} --block-size {block_size}")

    def pack(self, dev):
        process.run(f"blk-archive pack -a {self.dir} {dev}")
        return self.get_stream_id(dev)

    def pack_delta(self, old, old_id, new):
        process.run(f"blk-archive pack -a {self.dir} {new} --delta-stream {old_id} --delta-device {old}")
        return self.get_stream_id(new)

    def verify(self, dev):
        process.run(
            f"blk-archive verify -a {self.dir} --stream {self.get_stream_id(dev)} {dev}"
        )

    def get_stream_id(self, dev):
        name = os.path.basename(dev.path)
        (code, stdout, stderr) = process.run(f"blk-archive list -a {self.dir}")
        grep_output = [line for line in stdout.split("\n") if name in line]
        if len(grep_output) != 1:
            raise ValueError(f"couldn't find stream for {name}")

        # Run cut equivalent
        cut_output = [line.split(" ")[0] for line in grep_output]
        return cut_output[0]


def t_rolling_snaps(fix):
    fs_type = fs.Ext4
    thin_size = units.gig(8)

    archive_dir = "./test-archive"
    process.run(f"rm -rf {archive_dir}")
    archive = BlkArchive(archive_dir)

    ids = [0]
    
    with standard_pool(fix) as pool:
        with ps.new_thin(pool, thin_size, 0) as thin:
            linux_fs = fs_type(thin)
            linux_fs.format()

            with linux_fs.mount_and_chdir("./kernel_builds", discard=False):
                repo = git.Git.clone("../linux", "linux")
                repo.checkout(git.TAGS[0])

                index = 1

                # archive this via a snap
                with ps.new_snap(pool, thin_size, index, 0, thin) as snap:
                    id = archive.pack(snap)
                    ids.append(id)
                    archive.verify(snap)
                    index += 1

                # now we start archiving deltas
                for tag in git.TAGS[1:8]:
                    repo.checkout(tag)

                    with thin.pause():
                        pool.message(0, f"create_snap {index} 0")

                    with ps.thin(pool, thin_size, index - 1) as old_snap:
                        with ps.thin(pool, thin_size, index) as new_snap:
                            id = archive.pack_delta(old_snap, ids[index - 1], new_snap)
                            ids.append(id)
                            archive.verify(new_snap)

                    index += 1


# --------------------------------


def register(tests):
    tests.register_batch(
        "/blk-archive/",
        [
            ("rolling-snaps", t_rolling_snaps),
        ],
    )
