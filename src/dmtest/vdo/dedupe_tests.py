from dmtest.assertions import assert_near
from dmtest.vdo.utils import standard_vdo, wait_for_index
import dmtest.fs as fs
import dmtest.gendatablocks as generator
import dmtest.process as process
import dmtest.utils as utils
import dmtest.vdo.stats as stats

import os
import re
import logging as log
import time

def verify_dedupe(vdo, dedupe: float):
    # Wait for index to be online
    wait_for_index(vdo)
    # Do our usual wait on udev
    process.run("udevadm settle")

    # Get stats before any writing
    stats_pre = stats.vdo_stats(vdo)

    # Write 5000 4k blocks of specified dedupe
    br = generator.make_block_range(path=vdo.path, block_size=4096, block_count=5000)
    br.write(tag="tag1", dedupe=dedupe, compress=0.0, direct=True, fsync=True)
    # Grab the current stats and determine the difference between the two. This
    # will contain only the information related to just the writing. Compare
    # the expected dedupe rate vs the actual from the stats.
    stats_post = stats.vdo_stats(vdo)
    stats_delta = stats.make_delta_stats(stats_post, stats_pre)
    blocks_written = stats_delta["logicalBlocksUsed"]
    blocks_deduped = blocks_written - stats_delta["dataBlocksUsed"]
    actual = float(blocks_deduped / blocks_written)
    assert_near(actual, dedupe, 0.01)
    # Verify that the data on disk is what we wrote
    br.verify()

def t_dedupe0(fix):
    with standard_vdo(fix) as vdo:
        verify_dedupe(vdo, 0.0)

def t_dedupe50(fix):
    with standard_vdo(fix) as vdo:
        verify_dedupe(vdo, 0.50)

def t_dedupe75(fix):
    with standard_vdo(fix) as vdo:
        verify_dedupe(vdo, 0.75)


def register(tests):
    tests.register_batch(
        "/vdo/dedupe/",
        [
            ("dedupe0", t_dedupe0),
            ("dedupe50", t_dedupe50),
            ("dedupe75", t_dedupe75),
        ],
    )
