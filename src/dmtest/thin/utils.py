import dmtest.pool_stack as ps
import dmtest.utils as utils


def standard_stack(fix, **opts):
    cfg = fix.cfg
    if "data_size" not in opts:
        opts["data_size"] = utils.dev_size(cfg["data_dev"])
    return ps.PoolStack(cfg["data_dev"], cfg["metadata_dev"], **opts)


def standard_pool(fix, **opts):
    stack = standard_stack(fix, **opts)
    return stack.activate()
