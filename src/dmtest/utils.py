import dmtest.process as process
import dmtest.units as units
import logging as log
import os
import tempfile
import time
from contextlib import contextmanager


class TempFile:
    """
    Context manager that creates a temporary file and returns a file handle to
    the caller.

    The temporary file is automatically deleted when the context manager exits.

    Parameters:
    suffix (str): Optional file suffix to use for the temporary file.

    Yields:
    file: A file handle to the temporary file.

    Example:
    with with_temp_file(suffix='.txt') as (file, path):
        file.write('Hello, world!')
        file.flush()
        # Do something with the file...
    """

    def __init__(self, suffix=None):
        (fd, path) = tempfile.mkstemp(suffix)
        f = os.fdopen(fd, "w")
        self._f = f
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self._f.close()
        os.remove(self._path)

    @property
    def file(self):
        return self._f

    @property
    def path(self):
        return self._path


def retry_if_fails(func, *, max_retries=1, retry_delay=1):
    """
    Calls the given function and retries it until it succeeds or the maximum
    number of retries is reached.

    Parameters:
    func (function): The function to call.
    max_retries (int): The maximum number of times to retry the function.
    retry_delay (int): The number of seconds to wait between retries.

    Returns:
    The return value of the function if it succeeds.

    Raises:
    Exception: If the function fails after the maximum number of retries.
    """
    for i in range(max_retries):
        try:
            return func()
        except Exception:
            log.info(f"sleeping before retry, {retry_delay}s")
            time.sleep(retry_delay)
    return func()


def ensure_elapsed(thunk, seconds):
    """
    Calls the given function and then sleeps for long enough
    to ensure this function call takes 'seconds' duration.

    Returns:
    Whatever is returned by 'thunk'
    """
    start = time.time()
    r = thunk()
    elapsed = time.time() - start
    if elapsed < seconds:
        time.sleep(seconds - elapsed)
    r


def _dd_size(ifile, ofile):
    if ofile == "/dev/null":
        return dev_size(ifile)
    else:
        return dev_size(ofile)


def _dd_device(ifile, ofile, oflag, sectors):
    if not sectors:
        sectors = _dd_size(ifile, ofile)

    block_size = units.meg(64)
    (count, remainder) = divmod(sectors, block_size)

    if count > 0:
        process.run(
            f"dd if={ifile} of={ofile} {oflag} bs={block_size * 512} count={count}"
        )

    if remainder > 0:
        # deliberately missing out oflag because we don't want O_DIRECT
        process.run(
            f"dd if={ifile} of={ofile} bs=512 count={remainder} seek={count * block_size}"
        )


# A device could either be a str or a dmdev
def _to_path(dev):
    return str(dev)


def wipe_device(dev, sectors=None):
    _dd_device("/dev/zero", _to_path(dev), "oflag=direct", sectors)


def dev_size(dev):
    (_, stdout, _) = process.run(f"blockdev --getsz {_to_path(dev)}")
    return int(stdout)


@contextmanager
def timed(desc: str):
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        duration = end_time - start_time
        log.info(f"{desc} took {duration:.4f} seconds")
