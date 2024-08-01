import dmtest.device_mapper.dev as dmdev
import dmtest.device_mapper.table as table
import dmtest.device_mapper.targets as targets
import dmtest.utils as utils


class PoolStack:
    def __init__(self, data_dev, metadata_dev, **opts):
        self._data_dev = data_dev
        self._metadata_dev = metadata_dev
        self._data_size = opts.pop("data_size", utils.dev_size(data_dev))
        self._zero = opts.pop("zero", True)
        self._discard = opts.pop("discard", True)
        self._discard_passdown = opts.pop("discard_passdown", True)
        self._read_only = opts.pop("read_only", False)
        self._error_if_no_space = opts.pop("error_if_no_space", False)
        self._block_size = opts.pop("block_size", 64 * 2)
        self._low_water_mark = opts.pop("low_water_mark", 0)
        self._format = opts.pop("format", True)

        # if opts:
        # raise TypeError(f"Unsupported options {opts}")

        if self._format:
            utils.wipe_device(self._metadata_dev, 8)

    def _pool_table(self):
        return table.Table(
            targets.ThinPoolTarget(
                self._data_size,
                self._metadata_dev,
                self._data_dev,
                self._block_size,
                self._low_water_mark,
                self._zero,
                self._discard,
                self._discard_passdown,
                self._read_only,
                self._error_if_no_space,
            )
        )

    def activate(self):
        return dmdev.dev(self._pool_table())

    @property
    def block_size(self):
        return self._block_size


def _thin_table(pool, size, id, origin=None):
    return table.Table(targets.ThinTarget(size, pool.path, id, origin))


def thin(pool, size, id, origin=None, read_only=False):
    return dmdev.dev(_thin_table(pool, size, id, origin), read_only)


def new_thin(pool, size, id, origin=None, read_only=False):
    pool.message(0, f"create_thin {id}")
    return thin(pool, size, id, origin, read_only)


def thins(pool, size, *ids):
    def to_table(id):
        return _thin_table(pool, size, id)

    return dmdev.devs(*list(map(to_table, ids)))


def new_thins(pool, size, ids):
    for id in ids:
        pool.message(0, f"create_thin {id}")

    return thins(pool, size, *ids)


def new_snap(pool, size, id, old_id, pause_dev=None, origin=None, read_only=False):
    if pause_dev:
        with pause_dev.pause():
            pool.message(0, f"create_snap {id} {old_id}")
    else:
        pool.message(0, f"create_snap {id} {old_id}")

    return thin(pool, size, id, origin, read_only)
