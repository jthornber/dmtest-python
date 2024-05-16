from dmtest.assertions import assert_near, assert_equal
from dmtest.vdo.utils import standard_vdo, wait_for_index, discard, fsync
import dmtest.fs as fs
import dmtest.process as process
from dmtest.vdo.stats import vdo_stats

import json
import logging as log
import os
import tempfile
import time

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

# dmtest.units.kilo etc count in sectors, not bytes
kB = 1024
MB = 1024 * kB
GB = 1024 * MB

BLOCK_SIZE = 4 * kB

def run_fio(dev, size, offset, verify = False, stats = True):
    fio_config = fio_config_template.format(size=size,
                                            offset=offset,
                                            compress=74,
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

def wait_until_packer_only(vdo):
    """Waits until all the I/Os being processed by a VDO device are
    completed or waiting in the packer.

    Returns VDO stats collected after waiting. (dict, see vdo_stats)

    """
    while True:
        stats = vdo_stats(vdo)
        if stats['currentVIOsInProgress'] == stats['packer']['compressedFragmentsInPacker']:
            # We're done
            return stats
        time.sleep(0.001)

def t_compress(fix):
    size = 4 * MB
    size_in_blocks = int(size / BLOCK_SIZE)
    with standard_vdo(fix, compression="on") as vdo:
        process.run("udevadm settle")
        stats = vdo_stats(vdo)
        assert_equal(stats['dataBlocksUsed'], 0, 'data blocks used (init)')
        assert_equal(stats['hashLock']['dedupeAdviceValid'], 0,
                     'dedupe advice valid (init)')
        assert_equal(stats['biosIn']['write'], 0,
                     'write bios in (init)')
        log.info(f"data blocks used: {stats['dataBlocksUsed']}")
        wait_for_index(vdo)
        # No flushing here!
        run_fio(vdo, size, 0)
        # Flushing will cause I/Os in the packer to be pushed out;
        # there could be a bin with only one entry, which will get
        # written out uncompressed, or two entries, but (with the
        # consistent pattern of 3:1 compressibility) all the other
        # bins should hold three entries and get written out
        # compressed.
        #
        # However, any I/Os still in earlier stages of processing
        # (e.g., deduplication) that haven't yet reached the packer
        # stage will get written out uncompressed if the flush
        # notification reaches the packer first. In order to get
        # predictable rates for the test, we wait for all the I/Os we
        # sent to VDO either complete or stop in the packer.
        wait_until_packer_only(vdo)
        # And now we flush the I/Os left in the packer.
        fsync(vdo)
        stats = vdo_stats(vdo)
        assert_equal(stats['biosIn']['write'], size_in_blocks,
                     'write bios in (1st write)')
        expected_size = int((size_in_blocks + 2) / 3)
        # Some blocks in the packer may be written uncompressed when
        # we flush. That _should_ be only one, at most.
        assert_near(stats['dataBlocksUsed'], expected_size, 1,
                    'data blocks used (1st write)')
        assert_equal(stats['index']['postsNotFound'], size_in_blocks,
                     'posts not found (1st write)')
        assert_equal(stats['index']['postsFound'], 0,
                     'posts found (1st write)')
        assert_equal(stats['hashLock']['dedupeAdviceValid'], 0,
                     'dedupe advice valid (1st write)')
        # Write same data again, different location.
        # Confirm we deduplicate against compressed blocks.
        run_fio(vdo, size, size)
        stats2 = wait_until_packer_only(vdo)
        assert_equal(stats2['dataBlocksUsed'], stats['dataBlocksUsed'],
                     'data blocks used (2nd write)')
        assert_equal(stats2['index']['postsNotFound'], size_in_blocks,
                     'posts not found (2nd write)')
        assert_equal(stats2['index']['postsFound'], size_in_blocks,
                     'posts found (2nd write)')
        assert_equal(stats2['hashLock']['dedupeAdviceValid'],
                     size_in_blocks, 'dedupe advice valid (2nd write)')
        # Confirm we can read back compressed data correctly.
        run_fio(vdo, size, 0, verify=True, stats=False)
        # Check recovery of unreferenced compressed data.
        discard(vdo, 2 * size, 0)
        fsync(vdo)
        stats = vdo_stats(vdo)
        assert_equal(stats['dataBlocksUsed'], 0,
                     'data blocks used (discard)')

def register(tests):
    tests.register_batch(
        "/vdo/compress/",
        [
            ("compress", t_compress),
        ],
    )
