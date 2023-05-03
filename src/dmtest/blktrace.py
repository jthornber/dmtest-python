import logging as log
import re
import subprocess
import time
import dmtest.dependency_tracker as dep

from typing import List, NamedTuple


class UnknownBlkTraceCode(Exception):
    pass


class BlkTraceEvent(NamedTuple):
    event_type: str
    start_sector: int
    len_sector: int


def parse_events(txt: str, complete: bool):
    if complete:
        pattern = r"C ([DRW])[AS]? (\d+) (\d+) (\d+)"
    else:
        pattern = r"Q ([DRW])[AS]? (\d+) (\d+) (\d+)"

    events = []
    for line in txt.splitlines():
        ms = re.search(pattern, line)
        if ms:
            gs = ms.groups()
            events.append(BlkTraceEvent(gs[0], int(gs[1]), int(gs[2])))

    return events


class BlkTrace:
    def __init__(self, devs: List[str], complete=False):
        self._complete = complete

        blktrace_cmd = ["blktrace", "-o", "-"]

        for dev in devs:
            blktrace_cmd.extend(["-d", dev.path])

        if complete:
            blktrace_cmd.extend(["-a", "complete"])
        else:
            blktrace_cmd.extend(["-a", "queue"])

        dep.add_exe("blktrace")
        dep.add_exe("blkparse")
        log.info(f"starting blktrace: {blktrace_cmd}")
        self._blktrace = subprocess.Popen(
            blktrace_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        )

        blkparse_cmd = ["blkparse", "-f", '"%a %d %S %N %c\n"', "-i", "-"]
        log.info(f"starting blkparse: {blkparse_cmd}")

        self._blkparse = subprocess.Popen(
            blkparse_cmd,
            stdin=self._blktrace.stdout,
            stdout=subprocess.PIPE,
            universal_newlines=True
        )

    def __enter__(self):
        time.sleep(1)  # why do we need this?  udev again?
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._blktrace.stdout.close()
        self._blktrace.terminate()
        self._blktrace.wait()
        log.info("completed blktrace")

        stdout, _ = self._blkparse.communicate()
        self._events = parse_events(stdout, self._complete)

    @property
    def events(self):
        return self._events
