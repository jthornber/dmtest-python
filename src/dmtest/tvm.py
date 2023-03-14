import dmtest.device_mapper.targets as targets
import dmtest.device_mapper.table as table
import dmtest.utils as utils

from collections import namedtuple


DevSegment = namedtuple("DevSegment", ["dev", "offset", "length"])


class SegmentAllocationError(Exception):
    pass


class VolumeError(Exception):
    pass


def _allocate_segment(size, segs):
    """
    Allocates a single segment with length not greater than 'size'
    Returns a tuple of the newly allocated segment and the remains
    of the passed in segs.
    """
    if len(segs) == 0:
        raise SegmentAllocationError("Out of space in the segment allocator")
    s = segs.pop(0)
    if s.length > size:
        segs.insert(0, DevSegment(s.dev, s.offset + size, s.length - size))
        s = DevSegment(s.dev, s.offset, size)
    return (s, segs)


def _merge(segs):
    segs.sort(key=lambda seg: (seg.dev, seg.offset))

    merged = []
    s = segs.pop(0)
    while segs:
        n = segs.pop(0)
        if (n.dev == s.dev) and (n.offset == (s.offset + s.length)):
            # adjacent, we can merge them
            s = DevSegment(s.dev, s.offset, s.length + n.length)
        else:
            # non-adjacent, push what we've got
            merged.append(s)
            s = n
    if s:
        merged.append(s)

    return merged


class Allocator:
    def __init__(self):
        self._free_segments = []

    def allocate_segments(self, size, segment_predicate=None):
        if segment_predicate:
            segments = [s for s in self._free_segments if segment_predicate(s)]
        else:
            segments = self._free_segments

        result = []
        while size > 0:
            (s, segments) = _allocate_segment(size, segments)
            size -= s.length
            result.append(s)

        self._free_segments = segments
        return result

    def release_segments(self, segs):
        self._free_segments += segs
        self._free_segments = _merge(self._free_segments)

    def free_space(self):
        return sum([seg.length for seg in self._free_segments])


class Volume:
    def __init__(self, name, length):
        self._name = name
        self._length = length
        self._segments = []
        self._targets = []
        self._allocated = False

    def resize(self, allocator, new_length):
        raise NotImplementedError()

    def allocate(self, allocator):
        raise NotImplementedError()


def _segs_to_targets(segs):
    return [targets.LinearTarget(s.length, s.dev, s.offset) for s in segs]


class LinearVolume(Volume):
    def __init__(self, name, length):
        super().__init__(name, length)

    def resize(self, allocator, new_length):
        if not self._allocated:
            self._length = new_length
            return

        if new_length < self._length:
            raise NotImplementedError("reduce not implemented")

        new_segs = allocator.allocate_segments(new_length - self._length)
        self._segments += new_segs
        self._targets += _segs_to_targets(new_segs)
        self._length = new_length

    def allocate(self, allocator):
        self._segments = allocator.allocate_segments(self._length)
        self._targets = _segs_to_targets(self._segments)
        self._allocated = True


# This class manages the allocation aspect of volume management.
# It generates dm tables, but does _not_ manage activation.  Use
# the usual with dev(table) as thin: method for that
class VM:
    def __init__(self):
        self._allocator = Allocator()
        self._volumes = {}

    def add_allocation_volume(self, dev, offset=0, length=None):
        if not length:
            length = utils.dev_size(dev)

        self._allocator.release_segments([DevSegment(dev, offset, length)])

    def free_space(self):
        return self._allocator.free_space()

    def add_volume(self, vol):
        self._check_not_exist(vol._name)
        vol.allocate(self._allocator)
        self._volumes[vol._name] = vol

    def remove_volume(self, name):
        self._check_exists(name)
        vol = self._volumes[name]
        self._allocator.release_segments(vol._segments)
        del self._volumes[name]

    def resize(self, name, new_size):
        self._check_exists(name)
        self._volumes[name].resize(self._allocator, new_size)

    def segments(self, name):
        self._check_exists(name)
        return self._volumes[name]._segments

    def targets(self, name):
        self._check_exists(name)
        return self._volumes[name]._targets

    def table(self, name):
        return table.Table(*self.targets(name))

    def _check_not_exist(self, name):
        if name in self._volumes:
            raise VolumeError(f"Volume '{name}' already exists")

    def _check_exists(self, name):
        if name not in self._volumes:
            raise VolumeError(f"Volume '{name}' doesn't exist")
