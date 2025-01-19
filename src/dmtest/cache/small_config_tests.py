import unittest
import dmtest.units as units

from dmtest.cache_stack import ManagedCacheStack, CachePolicy

#----------------------------------------------------------------

def small_config(fix, policy):
    cfg = fix.cfg
    fast_dev = cfg["metadata_dev"]
    origin_dev = cfg["data_dev"]
    cache_dev = cfg.get("cache_dev", None)

    stack = ManagedCacheStack(
        fast_dev,
        origin_dev,
        cache_dev = cache_dev,
        format = True,
        metadata_size = units.meg(4),
        block_size = units.kilo(32),
        cache_size = units.kilo(50),
        target_len = units.kilo(50),
        policy = policy,
    )
    with stack.activate():
        pass

def t_small_config_mq(fix):
    small_config(fix, CachePolicy("mq"))

def t_small_config_smq(fix):
    small_config(fix, CachePolicy("smq"))

#----------------------------------------------------------------

def register(tests):
    tests.register_batch(
        "/cache/creation/",
        [
            ("small_config_mq", t_small_config_mq),
            ("small_config_smq", t_small_config_smq),
        ],
    )
