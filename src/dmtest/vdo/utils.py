import dmtest.vdo.vdo_stack as vs

def standard_stack(fix, **opts):
    cfg = fix.cfg
    return vs.VDOStack(cfg["data_dev"], **opts)


def standard_vdo(fix, **opts):
    stack = standard_stack(fix, **opts)
    return stack.activate()
