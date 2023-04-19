import os
import re

from typing import NamedTuple


class DataFile(NamedTuple):
    path: str
    size: int


class Dataset:
    def __init__(self, files):
        self.files = files

    def apply(self, count=None):
        if count is None or count >= len(self.files):
            for f in self.files:
                self.create_file(f.path, f.size)
        else:
            for i in range(count):
                f = self.files[i]
                self.create_file(f.path, f.size)

    @staticmethod
    def read(path):
        files = []
        with open(path, "r") as file:
            for line in file:
                m = re.match(r"(\S+)\s(\d+)", line)
                if m:
                    files.append(DataFile(m.group(1), int(m.group(2))))
        return Dataset(files)

    def create_file(self, path, size):
        dir_path, name = self.breakup_path(path)
        self.in_directory(dir_path, lambda: self.create_file_in_directory(name, size))

    @staticmethod
    def breakup_path(path):
        elements = path.split("/")
        return ["/".join(elements[:-1]), elements[-1]]

    @staticmethod
    def in_directory(dir_path, callback):
        os.makedirs(dir_path, exist_ok=True)
        os.chdir(dir_path)
        try:
            callback()
        finally:
            os.chdir("..")

    @staticmethod
    def create_file_in_directory(name, size):
        with open(name, "wb") as file:
            file.write(b"-" * size)
