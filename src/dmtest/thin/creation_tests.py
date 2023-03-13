from contextlib import contextmanager
import dmtest.device_mapper.dev as dmdev
import dmtest.pool_stack as ps
import dmtest.test_register
import dmtest.utils as utils
import dmtest.units as units
import logging as log


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


def t_create_lots_of_empty_snaps(fix):
    with standard_pool(fix) as pool:
        pool.message(0, f"create_thin 0")
        for id in range(1, 1000):
            pool.message(0, f"create_snap {id} 0")


def t_create_lots_of_recursive_snaps(fix):
    with standard_pool(fix) as pool:
        pool.message(0, f"create_thin 0")
        for id in range(1, 1000):
            pool.message(0, f"create_snap {id} {id - 1}")


def t_activate_thin_while_pool_suspended_fails(fix):
    failed = False
    volume_size = units.gig(4)
    with standard_pool(fix) as pool:
        pool.message(0, "create_thin 0")
        with dmdev.pause(pool):
            try:
                with ps.thin(pool, volume_size, 0):
                    # expect failure
                    pass
            except Exception:
                failed = True

    assert failed


def register(tests):
    tests.register_batch(
        "/thin/creation/",
        [
            ("lots-of-empty-thins", t_create_lots_of_empty_thins),
            ("lots-of-empty-snaps", t_create_lots_of_empty_snaps),
            ("lots-of-recursive-snaps", t_create_lots_of_recursive_snaps),
            (
                "activate-thin-while-pool-suspended-fails",
                t_activate_thin_while_pool_suspended_fails,
            ),
        ],
    )
