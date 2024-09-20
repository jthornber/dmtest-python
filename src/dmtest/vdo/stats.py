import os
import yaml

def _parse_vdo_stats(stats):
    return yaml.safe_load(stats)

def make_delta_stats(stats_post, stats_pre):
    """
    Given two stats dicts, the code creates a copy of post_stats except all
    its int fields values are the delta between post and pre.
    """
    if isinstance(stats_post, dict):
        node = {}
        for key, value in stats_post.items():
            node[key] = make_delta_stats(value, stats_pre[key])
        return node
    elif isinstance(stats_post, int):
        return stats_post - stats_pre
    return stats_post

def vdo_stats(dev):
    os.sync()
    stats = dev.message(0, "stats");
    return _parse_vdo_stats(stats)
