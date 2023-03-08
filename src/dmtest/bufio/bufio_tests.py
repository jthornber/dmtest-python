import dmtest.device_mapper.dev as dmdev
import dmtest.device_mapper.table as table
import dmtest.device_mapper.targets as targets
import dmtest.utils as utils
import enum
import logging as log
import mmap
import os
import struct


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


class BufioProgram:
    def __init__(self):
        self._bytes = b""

    def compile(self):
        return self._bytes[:]

    # FIXME: add labels
    def jmp(self, addr):
        self._bytes += struct.pack("BH", Instructions.I_JMP, addr)

    def bnz(self, addr, reg):
        self._bytes += struct.pack("BHB", Instructions.I_BNZ, addr, reg)

    def bz(self, addr, reg):
        self._bytes += struct.pack("BHB", Instructions.I_BZ, addr, reg)

    def halt(self):
        self._bytes += struct.pack("B", Instructions.I_HALT)

    def lit(self, val, reg):
        self._bytes += struct.pack("BIB", Instructions.I_LIT, val, reg)

    def sub(self, reg1, reg2):
        self._bytes += struct.pack("BBB", Instructions.I_SUB, reg1, reg2)

    def add(self, reg1, reg2):
        self._bytes += struct.pack("BBB", Instructions.I_SUB, reg1, reg2)

    def down_read(self, lock):
        self._bytes += struct.pack("BB", Instructions.I_DOWN_READ, lock)

    def up_read(self, lock):
        self._bytes += struct.pack("BB", Instructions.I_UP_READ, lock)

    def down_write(self, lock):
        self._bytes += struct.pack("BB", Instructions.I_DOWN_WRITE, lock)

    def up_write(self, lock):
        self._bytes += struct.pack("BB", Instructions.I_UP_READ, lock)

    def init_barrier(self):
        pass

    def wait_barrier(self):
        pass

    def new_buf(self, block, reg):
        self._bytes += struct.pack("BIB", Instructions.I_NEW_BUF, block, reg)

    def read_buf(self, block, reg):
        self._bytes += struct.pack("BIB", Instructions.I_READ_BUF, block, reg)

    def get_buf(self, block, reg):
        self._bytes += struct.pack("BIB", Instructions.I_GET_BUF, block, reg)

    def put_buf(self, reg):
        self._bytes += struct.pack("BB", Instructions.I_PUT_BUF, reg)

    def mark_dirty(self, reg):
        self._bytes += struct.pack("BB", Instructions.I_MARK_DIRTY, reg)

    def write_async(self):
        self._bytes += struct.pack("B", Instructions.I_WRITE_ASYNC)

    def write_sync(self):
        self._bytes += struct.pack("B", Instructions.I_WRITE_SYNC)

    def flush(self):
        self._bytes += struct.pack("B", Instructions.I_FLUSH)

    def forget(self, block):
        self._bytes += struct.pack("BI", Instructions.I_FORGET, block)

    def forget_range(self, block, len):
        self._bytes += struct.pack("BII", Instructions.I_FORGET_RANGE, block, len)


class BufioStack:
    def __init__(self, data_dev):
        self._data_dev = data_dev
        self._data_size = utils.dev_size(data_dev)

    def _table(self):
        return table.Table(targets.BufioTestTarget(self._data_size, self._data_dev))

    def activate(self):
        return dmdev.dev(self._table())


def t_create(fix):
    cfg = fix.cfg
    stack = BufioStack(cfg["data_dev"])
    with stack.activate() as _:
        pass


def t_empty_program(fix):
    cfg = fix.cfg
    stack = BufioStack(cfg["data_dev"])
    with stack.activate() as dev:
        code = BufioProgram()
        code.halt()

        fd = os.open(dev.path, os.O_DIRECT | os.O_WRONLY)
        try:
            # Map a single page of memory to the file
            page_size = os.sysconf("SC_PAGE_SIZE")
            with mmap.mmap(-1, page_size) as mem:
                mem.write(code.compile())
                os.write(fd, mem)
        finally:
            os.close(fd)


def register(tests):
    tests.register("/bufio/create", t_create)
    tests.register("/bufio/empty-program", t_empty_program)
