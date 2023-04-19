from dmtest.assertions import assert_raises
from dmtest.thin.utils import standard_stack, standard_pool
import dmtest.device_mapper.dev as dmdev
import dmtest.pool_stack as ps
import dmtest.tvm as tvm
import dmtest.units as units
import dmtest.utils as utils
import dmtest.process as process
import dmtest.dataset as dataset
import dmtest.fs as fs

import os


def t_overwrite_ext4(fix):
    thin_size = units.gig(4)
    ds = dataset.Dataset.read("compile-bench-datasets/dataset-unpatched")

    with standard_pool(fix) as pool:
        with ps.new_thin(pool, thin_size, 0) as thin:
            thin_fs = fs.Ext4(thin)
            thin_fs.format()
            dir = "./mnt1"
            with thin_fs.mounted(dir):
                with utils.change_dir(dir):
                    ds.apply(1000)

                with ps.new_snap(pool, thin_size, 1, 0, pause_dev=thin) as snap:
                    dir = "./mnt2"
                    thin_fs2 = fs.Ext4(snap)
                    with thin_fs2.mounted(dir):
                        ds.apply(1000)


def register(tests):
    tests.register_batch(
        "/thin/snapshot/",
        [
            ("overwrite-ext4", t_overwrite_ext4),
        ],
    )
