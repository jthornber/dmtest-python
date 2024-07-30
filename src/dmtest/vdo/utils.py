import dmtest.process as process
import dmtest.vdo.vdo_stack as vs
import dmtest.vdo.status as status
import logging as log

import json
import os
import tempfile
import time

# dmtest.units.kilo etc count in sectors, not bytes
kB = 1024
MB = 1024 * kB
GB = 1024 * MB

BLOCK_SIZE = 4 * kB

fio_config_template = """
[stuff]
randrepeat=1
ioengine=libaio
bs=4k
size={size}
rw=write
direct=1
scramble_buffers=1
buffer_compress_percentage={compress}
buffer_compress_chunk=4k
filename={filename}
iodepth=128
offset={offset}
end_fsync=0
{maybe_verify}
group_reporting=1
"""

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

def discard(dev, size, offset):
    process.run(f"blkdiscard -o {offset} -l {size} {dev}")

def fsync(dev):
    """Sync the specified device or file."""
    with open(dev, 'w') as thing:
        os.fsync(thing.fileno())

def run_fio(dev, size, offset, verify = False, stats = True, compression = 0):
    fio_config = fio_config_template.format(size=size,
                                            offset=offset,
                                            compress=compression,
                                            filename=str(dev),
                                            maybe_verify = "verify_only" if verify else "")
    log.info("fio config:\n" + fio_config)
    with tempfile.NamedTemporaryFile('w') as conf:
        conf.write(fio_config)
        conf.flush()
        if stats:
            with tempfile.NamedTemporaryFile('w') as out:
                process.run(f"fio {conf.name} --output={out.name} --output-format=json+")
                fio_out = json.load(open(out.name, 'r'))
                written = fio_out['jobs'][0]['write']['io_bytes'] # bytes
                duration = fio_out['jobs'][0]['write']['runtime'] # msec
                #log.info(fio_out)
                log.info(f"wrote {written} bytes in {duration} msec")
                return fio_out
        else:
            process.run(f"fio {conf.name}")
