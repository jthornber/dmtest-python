import os
import tempfile

from time import time, sleep


def temp_file(suffix=None):
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
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, 'w') as file:
            yield (file, path)
    finally:
        os.remove(path)


def retry_if_fails(func, max_retries=1, retry_delay=1):
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
            result = func()
            return result
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_delay)


def ensure_elapsed(thunk, seconds):
    """
    Calls the given function and then sleeps for long enough
    to ensure this function call takes 'seconds' duration.

    Returns:
    Whatever is returned by 'thunk'
    """
    start = time()
    r = thunk()
    elapsed = time() - start
    if elapsed < seconds:
        sleep(seconds - elapsed)
    r
