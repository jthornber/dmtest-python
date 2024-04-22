from dmtest.thin.utils import standard_pool
import dmtest.device_mapper.dev as dmdev
import dmtest.fs as fs
import dmtest.pool_stack as ps
import dmtest.process as process
import dmtest.tvm as tvm
import dmtest.units as units
import dmtest.utils as utils
import dmtest.tvm as tvm

import configparser
import logging as log
import tempfile
import time
import os

# ---------------------------------


def default_fio_config():
    config = configparser.ConfigParser()

    config["global"] = {
        "randrepeat": "1",
        "ioengine": "libaio",
        "bs": "4k",
        "ba": "4k",
        "size": "5G",
        "numjobs": "1",
        "direct": "1",
        "gtod_reduce": "1",
        "iodepth": "64",
        "runtime": "20",
    }

    config["mix"] = {
        "rw": "randrw",
        "timeout": "30",
    }

    return config


def run_fio(dev, fio_config):
    config_path = tempfile.mkstemp()[1]
    with open(config_path, "w") as cfg:
        fio_config.write(cfg, False)

    out_path = tempfile.mkstemp()[1]
    process.run(f"fio --filename={dev} --output={out_path} {config_path}")

    # read the contents of out_path and log it.
    with open(out_path, "r") as out_file:
        out_contents = out_file.read()
        log.info(out_contents)

    # unlink config_path and out_path
    os.unlink(config_path)
    os.unlink(out_path)


# ---------------------------------


def t_fio_thick(fix):
    size = units.gig(5)

    vm = tvm.VM()
    vm.add_allocation_volume(fix.cfg["data_dev"])
    vm.add_volume(tvm.LinearVolume("thick", size))

    with dmdev.dev(vm.table("thick")) as thick:
        time.sleep(1)
        run_fio(thick, default_fio_config())


def t_fio_thin(fix):
    size = units.gig(5)

    with standard_pool(fix) as pool:
        with ps.new_thin(pool, size, 0) as thin:
            time.sleep(1)
            run_fio(thin, default_fio_config())


def t_fio_thin_preallocated(fix):
    size = units.gig(5)

    with standard_pool(fix) as pool:
        with ps.new_thin(pool, size, 0) as thin:
            utils.wipe_device(thin)
            run_fio(thin, default_fio_config())


def register(tests):
    tests.register_batch(
        "/thin/fio/",
        [
            ("thick", t_fio_thick),
            ("thin", t_fio_thin),
            ("thin-preallocated", t_fio_thin_preallocated),
        ],
    )
