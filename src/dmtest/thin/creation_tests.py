import dmtest.test_register
import dmtest.pool_stack as ps
import dmtest.utils as utils

import dmtest.device_mapper.dev as dmdev

# -----------------------------------------


def standard_pool(fix, **opts):
    cfg = fix.cfg
    if "data_size" not in opts:
        opts["data_size"] = utils.dev_size(cfg["data_dev"])
    stack = ps.PoolStack(cfg["data_dev"], cfg["metadata_dev"], **opts)
    return stack.activate()


def t_create_lots_of_empty_thins(fix):
    with standard_pool(fix) as pool:
        for id in range(1000):
            pool.message(0, f"create_thin {id}")
            print(f"created thin {id}")


def register(tests):
    tests.register("/thin/creation/lots-of-empty-thins", t_create_lots_of_empty_thins)
