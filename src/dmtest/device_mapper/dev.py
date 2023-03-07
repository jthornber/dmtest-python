import logging
import random

import dmtest.device_mapper.interface as dm


class Dev:
    def __init__(self, name):
        self._name = name
        self._path = f"/dev/mapper/{name}"
        self._active_table = None
        dm.create(self._name)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.remove()
        # FIXME: put back in
        # self.post_remove_check()
        return None

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path

    def load(self, table):
        self._active_table = table
        dm.load(self._name, table)

    def load_ro(self, table):
        self._active_table = table
        dm.load_ro(self._name, table)

    def suspend(self):
        dm.suspend(self._name)

    def suspend_noflush(self):
        dm.suspend_noflush(self._name)

    def resume(self):
        dm.resume(self._name)

    def pause(self, callback):
        try:
            self.suspend()
            callback()
        finally:
            self.resume()

    def pause_noflush(self, callback):
        try:
            self.suspend_noflush()
            callback()
        finally:
            self.resume()

    def remove(self):
        dm.remove(self._name)

    def message(self, sector, *args):
        dm.message(self._name, sector, *args)

    def status(self, noflush=False):
        if noflush:
            dm.status(self._name, "--noflush")
        else:
            dm.status(self._name)

    def table(self):
        dm.table(self._name)

    def info(self):
        dm.info(self._name)

    def wait(self, event_nr):
        dm.wait(self._name, event_nr)

    def event_nr(self):
        output = dm.status(self._name, "-v")
        dm.extract_event_nr(output)


def dev(table, read_only=False):
    def _create_name():
        return f"test-dev-{random.randint(0, 1000000)}"

    dev = Dev(_create_name())
    if read_only:
        dev.load_ro(table)
    else:
        dev.load(table)
    dev.resume()
    return dev


# def dev(table, read_only=False):
# try:
# dev = Dev(lambda: _anon_dev(table, read_only), 1)
# try:
# yield dev
# finally:
# dev.remove()
# dev.post_remove_check()
# except Exception as e:
## Dev constructor failed, nothing we can do apart from log it
# logging.error(f"Failed to create device: {e}")
# pass


def devs(tables):
    """
    Creates one or more anonymous device-mapper devices and yields a tuple of
    the created devices.

    Args:
        tables (tuple): A tuple of table strings, one for each device to
                        create.

    Yields:
        tuple: A tuple of the created device-mapper devices.

    Raises:
        Exception: If any device-mapper devices fail to create.

    """
    try:
        devs = [Dev(lambda: _anon_dev(table), 1) for table in tables]
        try:
            yield tuple(devs)
        finally:
            for dev in devs:
                dev.remove()
                dev.post_remove_check()
    except Exception as e:
        # Dev constructor failed, nothing we can do apart from log it
        logging.error(f"Failed to create device: {e}")
        pass
