import string
from contextlib import contextmanager
from dmtest.thin.utils import standard_stack, standard_pool
import dmtest.units as units
import dmtest.pool_stack as ps
import dmtest.device_mapper.dev as dmdev
import dmtest.tvm as tvm
from dmtest.fs import Xfs

import uuid
import os
import dmtest.process as process
from enum import Enum

import json
import logging as log

import random


class BlkArchive:
    def __init__(self, directory, block_size=4096):
        self.dir = os.path.abspath(directory)
        process.run(f"blk-archive create -a {self.dir} --block-size {block_size}")

    def pack(self, device_or_file):
        stdout = process.run(f"blk-archive -j pack -a {self.dir} {device_or_file}")[1]
        result = json.loads(stdout)
        return result["stream_id"]

    def unpack(self, stream, dest):
        process.run(f"blk-archive -j unpack -a {self.dir} -s {stream} {dest}")

    def dump_stream(self, stream):
        return process.run(f"blk-archive -j dump-stream -a {self.dir} -s {stream}")

    def pack_delta(self, old, old_id, new):
        stdout = process.run(f"blk-archive pack -a {self.dir} {new} --delta-stream {old_id} --delta-device {old}")[1]
        result = json.loads(stdout)
        return result["stream_id"]

    def verify(self, dev_or_file, stream=None):

        if stream is None:
            stream = self.get_stream_id(dev_or_file)
        process.run(
            f"blk-archive verify -a {self.dir} --stream {stream} {dev_or_file}"
        )

    def get_stream_id(self, dev_or_file):
        if type(dev_or_file) is not str:
            name = os.path.basename(dev_or_file.path)
        else:
            name = os.path.basename(dev_or_file)
        (code, stdout, stderr) = process.run(f"blk-archive list -a {self.dir}")
        grep_output = [line for line in stdout.split("\n") if name in line]
        if len(grep_output) != 1:
            raise ValueError(f"couldn't find stream for {name}")

        # Run cut equivalent
        cut_output = [line.split(" ")[0] for line in grep_output]
        return cut_output[0]
