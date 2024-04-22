from dmtest.thin.utils import standard_pool
import dmtest.device_mapper.dev as dmdev
import dmtest.pool_stack as ps
import dmtest.tvm as tvm
import dmtest.units as units
import dmtest.utils as utils
import dmtest.fio as fio

import time

# ---------------------------------


def t_fio_thick(fix):
    size = units.gig(5)

    vm = tvm.VM()
    vm.add_allocation_volume(fix.cfg["data_dev"])
    vm.add_volume(tvm.LinearVolume("thick", size))

    with dmdev.dev(vm.table("thick")) as thick:
        time.sleep(1)
        fio.run_fio(thick, fio.default_fio_config())


def t_fio_thin(fix):
    size = units.gig(5)

    with standard_pool(fix) as pool:
        with ps.new_thin(pool, size, 0) as thin:
            time.sleep(1)
            fio.run_fio(thin, fio.default_fio_config())


def t_fio_thin_preallocated(fix):
    size = units.gig(5)

    with standard_pool(fix) as pool:
        with ps.new_thin(pool, size, 0) as thin:
            utils.wipe_device(thin)
            fio.run_fio(thin, fio.default_fio_config())


def register(tests):
    tests.register_batch(
        "/thin/fio/",
        [
            ("thick", t_fio_thick),
            ("thin", t_fio_thin),
            ("thin-preallocated", t_fio_thin_preallocated),
        ],
    )