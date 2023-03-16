import dmtest.device_mapper.dev as dmdev
import dmtest.device_mapper.table as table
import dmtest.device_mapper.targets as targets
import dmtest.units as units
import dmtest.utils as utils
import enum
import logging as log
import mmap
import os
import random
import struct
import threading
import time

from contextlib import contextmanager


class Instructions(enum.IntEnum):
    I_JMP = 0
    I_BNZ = 1
    I_BZ = 2
    I_HALT = 3
    I_LIT = 4
    I_SUB = 5
    I_ADD = 6
    I_DOWN_READ = 7
    I_UP_READ = 8
    I_DOWN_WRITE = 9
    I_UP_WRITE = 10
    I_INIT_BARRIER = 11
    I_WAIT_BARRIER = 12
    I_NEW_BUF = 13
    I_READ_BUF = 14
    I_GET_BUF = 15
    I_PUT_BUF = 16
    I_MARK_DIRTY = 17
    I_WRITE_ASYNC = 18
    I_WRITE_SYNC = 19
    I_FLUSH = 20
    I_FORGET = 21
    I_FORGET_RANGE = 22
    I_LOOP = 23
    I_STAMP = 24
    I_VERIFY = 25
    I_CHECKPOINT = 26


class BufioProgram:
    def __init__(self):
        self._bytes = b""
        self._labels = {}
        self._reg_alloc = 0

    def compile(self):
        return self._bytes[:]

    def alloc_reg(self):
        reg = self._reg_alloc
        self._reg_alloc += 1
        return reg

    def label(self):
        return len(self._bytes)

    def jmp(self, addr):
        self._bytes += struct.pack("=BH", Instructions.I_JMP, addr)

    def bnz(self, addr, reg):
        self._bytes += struct.pack("=BHB", Instructions.I_BNZ, addr, reg)

    def bz(self, addr, reg):
        self._bytes += struct.pack("=BHB", Instructions.I_BZ, addr, reg)

    def halt(self):
        self._bytes += struct.pack("=B", Instructions.I_HALT)

    def lit(self, val, reg):
        self._bytes += struct.pack("=BIB", Instructions.I_LIT, val, reg)

    def sub(self, reg1, v):
        self._bytes += struct.pack("=BBB", Instructions.I_SUB, reg1, v)

    def add(self, reg1, v):
        self._bytes += struct.pack("=BBB", Instructions.I_ADD, reg1, v)

    def inc(self, reg1):
        self.add(reg1, 1)

    def down_read(self, lock):
        self._bytes += struct.pack("=BB", Instructions.I_DOWN_READ, lock)

    def up_read(self, lock):
        self._bytes += struct.pack("=BB", Instructions.I_UP_READ, lock)

    def down_write(self, lock):
        self._bytes += struct.pack("=BB", Instructions.I_DOWN_WRITE, lock)

    def up_write(self, lock):
        self._bytes += struct.pack("=BB", Instructions.I_UP_WRITE, lock)

    def init_barrier(self):
        pass

    def wait_barrier(self):
        pass

    def new_buf(self, block_reg, dest_reg):
        self._bytes += struct.pack("=BBB", Instructions.I_NEW_BUF, block_reg, dest_reg)

    def read_buf(self, block_reg, dest_reg):
        self._bytes += struct.pack("=BBB", Instructions.I_READ_BUF, block_reg, dest_reg)

    def get_buf(self, block_reg, dest_reg):
        self._bytes += struct.pack("=BBB", Instructions.I_GET_BUF, block_reg, dest_reg)

    def put_buf(self, reg):
        self._bytes += struct.pack("=BB", Instructions.I_PUT_BUF, reg)

    def mark_dirty(self, reg):
        self._bytes += struct.pack("=BB", Instructions.I_MARK_DIRTY, reg)

    def write_async(self):
        self._bytes += struct.pack("=B", Instructions.I_WRITE_ASYNC)

    def write_sync(self):
        self._bytes += struct.pack("=B", Instructions.I_WRITE_SYNC)

    def flush(self):
        self._bytes += struct.pack("=B", Instructions.I_FLUSH)

    def forget(self, block):
        self._bytes += struct.pack("=BI", Instructions.I_FORGET, block)

    def forget_range(self, block, len):
        self._bytes += struct.pack("=BII", Instructions.I_FORGET_RANGE, block, len)

    def loop(self, addr, reg):
        self._bytes += struct.pack("=BHB", Instructions.I_LOOP, addr, reg)

    def stamp(self, buf_reg, pattern_reg):
        self._bytes += struct.pack("=BBB", Instructions.I_STAMP, buf_reg, pattern_reg)

    def verify(self, buf_reg, pattern_reg):
        self._bytes += struct.pack("=BBB", Instructions.I_VERIFY, buf_reg, pattern_reg)

    def checkpoint(self, reg):
        self._bytes += struct.pack("=BB", Instructions.I_CHECKPOINT, reg)


@contextmanager
def loop(p, nr_times):
    loop_counter = p.alloc_reg()
    p.lit(nr_times, loop_counter)
    addr = p.label()
    try:
        yield p
    finally:
        p.loop(addr, loop_counter)


def exec_program(dev, program):
    bytes = program.compile()
    if len(bytes) > 4096:
        raise ValueError("buffer is too large")

    fd = os.open(dev.path, os.O_DIRECT | os.O_WRONLY)
    try:
        # Map a single page of memory to the file
        page_size = os.sysconf("SC_PAGE_SIZE")
        with mmap.mmap(-1, page_size) as mem:
            mem.write(bytes)
            with utils.timed("bufio program"):
                os.write(fd, mem)
    finally:
        os.close(fd)


class Code:
    def __init__(self, thread_set):
        self._thread_set = thread_set
        self._code = BufioProgram()

    def __enter__(self):
        return self._code

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            return

        self._code.halt()
        self._thread_set.add_thread(self._code)


class ThreadSet:
    def __init__(self, dev):
        self._dev = dev
        self._programs = []

    def program(self):
        return Code(self)

    def add_thread(self, code):
        self._programs.append(code)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            return

        threads = []

        for code in self._programs:
            tid = threading.Thread(target=exec_program, args=(self._dev, code))
            threads.append(tid)

        for tid in threads:
            tid.start()

        for tid in threads:
            tid.join()


# Activate bufio test device and create a thread set
@contextmanager
def bufio_threads(data_dev):
    data_size = utils.dev_size(data_dev)
    t = table.Table(targets.BufioTestTarget(data_size, data_dev))
    with bufio_params_tracker():
        with dmdev.dev(t) as dev:
            with ThreadSet(dev) as thread_set:
                yield thread_set


def _sys_param(name: str) -> str:
    return f"/sys/module/dm_bufio/parameters/{name}"


def read_sys_param(name: str) -> int:
    with open(_sys_param(name), "r") as file:
        return int(file.read().strip())


def write_sys_param(name: str, value: str):
    with open(_sys_param(name), "w") as file:
        return file.write(value)


class BufioParams:
    @property
    def peak_allocated(self):
        return read_sys_param("peak_allocated_bytes")

    @property
    def current_allocated(self):
        return read_sys_param("current_allocated_bytes")

    @property
    def max_cache_size(self):
        return read_sys_param("max_cache_size_bytes")

    @max_cache_size.setter
    def max_cache_size(self, value):
        return write_sys_param("max_cache_size_bytes", str(value))


@contextmanager
def bufio_params_tracker():
    def worker(stop_event):
        p = BufioParams()
        while not stop_event.is_set():
            log.info(
                f"bufio cache size: {p.current_allocated // (1024 * 1024)}m/{p.max_cache_size // (1024 * 1024)}m"
            )
            time.sleep(0.5)

    stop_event = threading.Event()
    tid = threading.Thread(target=worker, args=(stop_event,))
    try:
        tid.start()
        yield
    finally:
        stop_event.set()
        tid.join()


# -----------------------------------------------


def t_create(fix):
    with bufio_threads(fix.cfg["data_dev"]):
        pass


def t_empty_program(fix):
    with bufio_threads(fix.cfg["data_dev"]) as thread_set:
        with thread_set.program():
            pass


def do_new_buf(p, base):
    block = p.alloc_reg()
    buf = p.alloc_reg()

    p.lit(base, block)

    with loop(p, 1024) as p:
        p.new_buf(block, buf)
        p.put_buf(buf)
        p.inc(block)


def t_new_buf(fix):
    nr_threads = 16
    nr_gets = 1024

    with bufio_threads(fix.cfg["data_dev"]) as thread_set:
        for t in range(nr_threads):
            with thread_set.program() as p:
                do_new_buf(p, t * nr_gets)


def t_stamper(fix):
    with bufio_threads(fix.cfg["data_dev"]) as thread_set:
        with thread_set.program() as p:
            block = p.alloc_reg()
            buf = p.alloc_reg()
            pattern = p.alloc_reg()

            p.lit(0, block)
            p.lit(random.randint(0, 1024), pattern)

            with loop(p, 1024) as p:
                # stamp
                p.new_buf(block, buf)
                p.stamp(buf, pattern)
                p.mark_dirty(buf)
                p.put_buf(buf)

                # write
                p.write_sync()
                p.forget(block)

                # re-read and verify
                p.read_buf(block, buf)
                p.verify(buf, pattern)
                p.put_buf(buf)

                p.inc(block)
                p.inc(pattern)


def do_stamper(p, base):
    block = p.alloc_reg()
    buf = p.alloc_reg()
    pattern = p.alloc_reg()

    p.lit(base, block)
    p.lit(random.randint(0, 1024), pattern)

    with loop(p, 1024) as p:
        # stamp
        p.new_buf(block, buf)
        p.stamp(buf, pattern)
        p.mark_dirty(buf)
        p.put_buf(buf)

        # write
        p.write_sync()
        p.forget(block)

        # re-read and verify
        p.read_buf(block, buf)
        p.verify(buf, pattern)
        p.put_buf(buf)

        p.inc(block)
        p.inc(pattern)


def t_many_stampers(fix):
    nr_threads = 16
    nr_gets = 1024

    with bufio_threads(fix.cfg["data_dev"]) as thread_set:
        for t in range(nr_threads):
            with thread_set.program() as p:
                do_stamper(p, t * nr_gets)


def t_writeback_nothing(fix):
    data_dev = fix.cfg["data_dev"]
    nr_blocks = units.meg(512) // units.kilo(4)

    with bufio_threads(data_dev) as thread_set:
        with thread_set.program() as p:
            block = p.alloc_reg()
            buf = p.alloc_reg()

            p.lit(0, block)
            p.checkpoint(0)

            # read data, but don't dirty it
            with loop(p, nr_blocks) as p:
                p.read_buf(block, buf)
                p.put_buf(buf)
                p.inc(block)

            # write back, should do nothing
            p.checkpoint(1)
            p.write_sync()
            p.checkpoint(2)


def t_writeback_many(fix):
    data_dev = fix.cfg["data_dev"]
    nr_blocks = units.gig(8) // units.kilo(4)

    with bufio_threads(data_dev) as thread_set:
        with thread_set.program() as p:
            block = p.alloc_reg()
            buf = p.alloc_reg()

            p.lit(0, block)
            p.checkpoint(0)

            # mark first 8G as dirty
            with loop(p, nr_blocks) as p:
                p.new_buf(block, buf)
                p.mark_dirty(buf)
                p.put_buf(buf)
                p.inc(block)

            # write back
            p.checkpoint(1)
            p.write_sync()
            p.checkpoint(2)


def t_hotspots(fix):
    nr_hotspots = 16

    # size in 4k blocks
    region_size = units.meg(4) // units.kilo(4)
    regions = [(n * region_size, (n + 1) * region_size) for n in range(0, nr_hotspots)]

    with bufio_threads(fix.cfg["data_dev"]) as thread_set:
        for b, e in regions:
            with thread_set.program() as p:
                block = p.alloc_reg()
                buf = p.alloc_reg()

                with loop(p, 16) as p:
                    p.lit(b, block)
                    with loop(p, e - b) as p:
                        p.read_buf(block, buf)
                        p.put_buf(buf)
                        p.inc(block)


def register(tests):
    tests.register("/bufio/create", t_create)
    tests.register("/bufio/empty-program", t_empty_program)
    tests.register("/bufio/new-buf", t_new_buf)
    tests.register("/bufio/stamper", t_stamper)
    tests.register("/bufio/many-stampers", t_many_stampers)
    tests.register("/bufio/writeback-nothing", t_writeback_nothing)
    tests.register("/bufio/writeback-many", t_writeback_many)
    tests.register("/bufio/hotspots", t_hotspots)
