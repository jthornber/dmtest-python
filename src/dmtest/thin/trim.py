from typing import List, Tuple, Callable, Dict, Iterator
import os
import random
from contextlib import contextmanager

from dmtest.assertions import assert_raises
from dmtest.thin.utils import standard_stack, standard_pool
from dmtest.thin.status import pool_status
from dmtest.device_mapper.dev import Dev

import dmtest.blktrace as bt
import dmtest.pool_stack as ps
import dmtest.units as units
import dmtest.utils as utils
import dmtest.thin.xml as xml
import dmtest.thin.xml_combinators as xmlc
from dmtest.process import run

# ---------------------------------

def sectors_to_blocks(n):
    return n / 128

def restored_pool(fix, data_block_size: int, nr_data_blocks: int, patterns: Dict[int, xmlc.Pattern], **opts) -> Dev:
    data_dev = fix.cfg['data_dev']
    metadata_dev = fix.cfg['metadata_dev']
    pool = xmlc.generate_pool_with_thins(data_block_size, nr_data_blocks, patterns)

    # Create a temporary file for the xml metadata
    with utils.TempFile("xml") as tf:
        xml.write_xml(pool, tf.path)
        tf.file.flush()
        tf.no_delete()

        # Restore the xml to the metadata dev using thin_restore
        run(f"thin_restore -i {tf.path} -o {metadata_dev}")

    # Bring up the pool
    cfg = fix.cfg
    if "data_size" not in opts:
        opts["data_size"] = utils.dev_size(cfg["data_dev"])
    opts["format"] = False
    pool = ps.PoolStack(data_dev, metadata_dev, **opts)

    return pool.activate()

# trim messages should have no effect, since all space is allocated
def t_fully_allocated(fix):
    data_block_size = 128
    gig = sectors_to_blocks(units.gig(1));

    # a single, fully allocated thin device
    patterns = {0: xmlc.allocate(gig)}

    with restored_pool(fix, data_block_size, gig, patterns) as pool:
        with ps.thin(pool, units.gig(1), 0) as thin:
            trace = bt.BlkTrace([thin.path])
            with trace:
                end_block = gig / data_block_size
                pool.message(0, f"trim 0 {end_block}")
            print(trace)

        #os.remove(xml_file)

# def t_trim_fragmented_thin_incrementally(fix):
#     pattern = create_fragmented_pattern(total_length=units.gig(1), fragment_size=units.meg(1), allocation_ratio=0.7)
#     xml_file = "fragmented_thin.xml"
#     write_xml(generate_xml(pattern), xml_file)
#
#     with standard_pool(fix) as pool:
#         pool.message(0, f"load {xml_file}")
#         with ps.thin(pool, units.gig(1), 0) as thin:
#             initial_used = pool.status()[1]['used']
#
#             # Trim in 10MB increments
#             for start in range(0, units.gig(1), units.meg(10)):
#                 pool.message(0, f"trim {start} {start + units.meg(10)}")
#
#             final_used = pool.status()[1]['used']
#             # Verify that space has been freed
#             assert final_used < initial_used
#
#     os.remove(xml_file)
#
# def t_trim_hotspot_thin_incrementally(fix):
#     pattern = create_hotspot_pattern(total_length=units.gig(1), hotspot_count=5, 
#                                      hotspot_size=units.meg(50), hotspot_allocation_ratio=0.9, 
#                                      base_allocation_ratio=0.3)
#     xml_file = "hotspot_thin.xml"
#     write_xml(generate_xml(pattern), xml_file)
#
#     with standard_pool(fix) as pool:
#         pool.message(0, f"load {xml_file}")
#         with ps.thin(pool, units.gig(1), 0) as thin:
#             initial_used = pool.status()[1]['used']
#
#             # Trim each hotspot in 1MB increments
#             for hotspot_start in range(0, units.gig(1), units.meg(200)):
#                 for offset in range(0, units.meg(50), units.meg(1)):
#                     start = hotspot_start + offset
#                     pool.message(0, f"trim {start} {start + units.meg(1)}")
#
#             final_used = pool.status()[1]['used']
#             # Verify that space has been freed
#             assert final_used < initial_used
#
#     os.remove(xml_file)
#
# def t_trim_time_based_thin_incrementally(fix):
#     pattern = create_time_based_pattern(total_length=units.gig(1), time_periods=10, allocation_increase=0.05)
#     xml_file = "time_based_thin.xml"
#     write_xml(generate_xml(pattern), xml_file)
#
#     with standard_pool(fix) as pool:
#         pool.message(0, f"load {xml_file}")
#         with ps.thin(pool, units.gig(1), 0) as thin:
#             initial_used = pool.status()[1]['used']
#
#             # Trim in 5MB increments, focusing on the latter half (more densely allocated)
#             for start in range(units.meg(500), units.gig(1), units.meg(5)):
#                 pool.message(0, f"trim {start} {start + units.meg(5)}")
#
#             final_used = pool.status()[1]['used']
#             # Verify that significant space has been freed
#             assert final_used < initial_used * 0.9
#
#     os.remove(xml_file)
#
# def t_trim_with_concurrent_writes(fix):
#     with standard_pool(fix) as pool:
#         with ps.thin(pool, units.gig(1), 0) as thin:
#             utils.wipe_device(thin)
#             initial_used = pool.status()[1]['used']
#
#             # Simulate concurrent trimming and writing
#             for i in range(100):
#                 # Trim a small region
#                 trim_start = random.randint(0, units.gig(1) - units.meg(1))
#                 pool.message(0, f"trim {trim_start} {trim_start + units.meg(1)}")
#
#                 # Write to a different region
#                 write_start = random.randint(0, units.gig(1) - units.meg(1))
#                 utils.write_blocks(thin, write_start, units.meg(1))
#
#             final_used = pool.status()[1]['used']
#             # Verify that the final usage is different from the initial
#             # (it could be more or less depending on the random trim/write balance)
#             assert final_used != initial_used
#
# def t_repeated_trim_same_region(fix):
#     with standard_pool(fix) as pool:
#         with ps.thin(pool, units.gig(1), 0) as thin:
#             utils.wipe_device(thin)
#             initial_used = pool.status()[1]['used']
#
#             # Repeatedly trim the same region
#             for _ in range(100):
#                 pool.message(0, f"trim 0 {units.meg(10)}")
#
#             final_used = pool.status()[1]['used']
#             # Verify that space has been freed
#             assert final_used < initial_used
#             # Verify that the trimmed region is empty
#             assert utils.region_empty(thin, 0, units.meg(10))
#

def register(tests):
    tests.register_batch(
        "/thin/trim/",
        [
            ("fully-allocated", t_fully_allocated),
            # ("fragmented-thin-incrementally", t_trim_fragmented_thin_incrementally),
            # ("trim-hotspot-thin-incrementally", t_trim_hotspot_thin_incrementally),
            # ("trim-time-based-thin-incrementally", t_trim_time_based_thin_incrementally),
            # ("trim-with-concurrent-writes", t_trim_with_concurrent_writes),
            # ("repeated-trim-same-region", t_repeated_trim_same_region),
        ],
    )
