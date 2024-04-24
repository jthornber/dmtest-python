import dmtest.vdo.vdo_stack as vs
import dmtest.vdo.status as status

import time

def standard_stack(fix, **opts):
    cfg = fix.cfg
    return vs.VDOStack(cfg["data_dev"], **opts)


def standard_vdo(fix, **opts):
    stack = standard_stack(fix, **opts)
    return stack.activate()

def wait_for_index(dev):
    count = 0;
    while (count < 30 and status.vdo_status(dev)["index-state"] != "online"):
        count += 1
        time.sleep(1)
    if status.vdo_status(dev)["index-state"] != "online":
        raise AssertionError("VDO not online within 30 seconds")
