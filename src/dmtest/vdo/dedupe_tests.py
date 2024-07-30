from dmtest.assertions import assert_equal, assert_near
from dmtest.vdo.utils import BLOCK_SIZE, fsync, run_fio, standard_vdo, wait_for_index
import dmtest.gendatablocks as generator
import dmtest.process as process
import dmtest.vdo.stats as stats

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

def t_dedupeWithOffsetAndRestart(fix):
    """
    Write the same data at two offsets and ensure that VDO statistics reflect
    the appropriate values

    After writing the data for the first round:
        dataBlocksUsed should equal the total number of blocks written
        entriesIndexed should equal the total number of blocks written

    After writing the same data a second time:
        dedupeAdviceValid should equal the number of blocks written originally
    """
    block_count = 5000
    size = int(block_count * BLOCK_SIZE)
    with standard_vdo(fix) as vdo:
        # Write {size} data at 0 offset
        run_fio(vdo, size, 0)

        # Ensure data is stable before checking stats
        fsync(vdo)

        # Verify first round statistics equal total data written
        vdo_stats_before = stats.vdo_stats(vdo)
        assert_equal(vdo_stats_before['dataBlocksUsed'], block_count)
        assert_equal(vdo_stats_before['index']['entriesIndexed'], block_count)

        # Write {size} data at {size} offset
        run_fio(vdo, size, size)

        # Ensure data is stable before checking stats
        fsync(vdo)

        # Verify second round statistics reflect effective deduplication
        vdo_stats_after = stats.vdo_stats(vdo)
        assert_equal(vdo_stats_after['hashLock']['dedupeAdviceValid'], block_count)

    # Re-assemble the VDO device, but this time without formatting
    with standard_vdo(fix, format=False) as vdo:
        # Verify the first set of data is still correct
        run_fio(vdo, size, 0, verify=True)

        # Verify the second set of data is still correct
        run_fio(vdo, size, size, verify=True)

def register(tests):
    tests.register_batch(
        "/vdo/dedupe/",
        [
            ("dedupe0", t_dedupe0),
            ("dedupe50", t_dedupe50),
            ("dedupe75", t_dedupe75),
            ("dedupeWithOffsetAndRestart", t_dedupeWithOffsetAndRestart),
        ],
    )
