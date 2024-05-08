import dmtest.process as process
import dmtest.log_label as ll
import dmtest.units as units
from typing import List

import configparser
import logging as log
import random
import tempfile
import os

# ---------------------------------


def uniform_config(name: str, iolog=False):
    config = configparser.ConfigParser()

    config["global"] = {
        "randrepeat": "1",
        "ioengine": "libaio",
        "bs": "4k",
        "ba": "4k",
        "size": "5G",
        "io_size": "1G",
        "numjobs": "1",
        "direct": "1",
        "iodepth": "64",
    }

    config["mix"] = {
        "name": name,
        "rw": "randrw",
    }

    if iolog:
        config["mix"]["write_iolog"] = "iolog.out"

    return config

# ---------------------------------

def validate_zones(zones):
    total_io = 0
    total_dev = 0
    for percent_io, percent_dev in zones:
        total_io += percent_io
        total_dev += percent_dev

    total_io = int(total_io)
    total_dev = int(total_dev)

    if total_io != 100:
        print(f"total_io = {total_io}")
        assert total_io == 100

    if total_dev != 100:
        print(f"total_dev = {total_dev}")
        assert total_dev == 100

def build_zone_str(zones):
    r = "zoned"

    for percent_io, percent_dev in zones:
        r += f":{percent_io}/{percent_dev}"

    return r

# Scales all values in a array such that they sum to @new_total
def scale_ints(values: List[int], new_total: int) -> List[int]:
    current_sum = sum(values)
    if current_sum == 0:
        raise ValueError("Sum of input values is zero; cannot scale to new total.")
    
    scale_factor = new_total / current_sum    
    scaled_values = [int(round(x * scale_factor)) for x in values]
    
    # Correct potential rounding errors to ensure the sum matches exactly
    scaled_sum = sum(scaled_values)
    difference = new_total - scaled_sum    
    # Adjust the scaled values to match the exact new total
    i = 0
    while difference != 0:
        if difference > 0:
            scaled_values[i] += 1
            difference -= 1
        elif difference < 0:
            scaled_values[i] -= 1
            difference += 1
        i = (i + 1) % len(values)
    
    return scaled_values

# Returns an array of [[<begin, end>]] runs that covers @percent of @nr_regions
def random_runs(nr_select: int, nr_regions: int):
    # pick ramdom regions
    regions = set()
    for _ in range(int(nr_select)):
        b = random.randint(0, nr_regions)
        while b in regions:
            b = random.randint(0, nr_regions)

        regions.add(b)

    def set_run(begin):
        for b in range(begin + 1, nr_regions):
            if not b in regions:
                return b;

        return nr_regions

    def empty_run(begin):
        for b in range(begin + 1, nr_regions):
            if b in regions:
                return b;

        return nr_regions

    # build runs
    runs = []
    b = 0

    while b < nr_regions:
        if b in regions:
            e = set_run(b)
            runs.append([b, e])
        else:
            e = empty_run(b)

        b = e

    return runs

# A scattering of small zones to simulate deltas in a snapshot 
# @percent is the percentage of the device that you want to recieve io
def scatter_zones(percent: int):
    nr_regions = 100
    nr_select = int((nr_regions * percent) / 100)
    runs = random_runs(nr_select, nr_regions)

    io_percents = []
    dev_percents = []
    last = 0
    for b, e in runs:
        if last < b:
            io_percents.append(0)
            dev_percents.append(b - last)

        io_percents.append(e - b)
        dev_percents.append(e - b)
        last = e

    io_percents = scale_ints(io_percents, 100)

    zones = list(zip(io_percents, dev_percents))
    print(zones)
    return zones


def zoned_config(name, io_size, rwmixread, zones, iolog=False):
    validate_zones(zones)

    config = configparser.ConfigParser()

    config["global"] = {
        "randrepeat": "1",
        "ioengine": "libaio",
        "bs": "4k",
        "ba": "4k",
        "numjobs": "1",
        "direct": "1",
        "iodepth": "64",
    }

    config[name] = {
        "rw": "randrw",
        "rwmixread": str(rwmixread),
        "random_distribution": build_zone_str(zones),
        "io_size": str(io_size * units.SECTOR_SIZE),
    }

    if iolog:
        config[name]["write_iolog"] = "iolog.out"

    return config


# ---------------------------------

def default_fio_config(name: str):
    zones = scatter_zones(75)
    return zoned_config(name, units.gig(1), 50, zones, True)


# ---------------------------------


def log_contents(banner, path):
    with open(path, "r") as file:
        log.info(f"{banner}:\n{file.read()}")


def run_fio(dev, fio_config):
    with ll.log_label("fio"):
        config_path = tempfile.mkstemp()[1]
        with open(config_path, "w") as cfg:
            fio_config.write(cfg, False)

        with ll.log_label("config"):
            log_contents("fio config file", config_path)

        out_path = tempfile.mkstemp()[1]
        process.run(
            f"fio --filename={dev} --output={out_path} --output-format=json+ {config_path}"
        )

        with ll.log_label("results"):
            log_contents("fio output", out_path)

        # unlink config_path and out_path
        os.unlink(config_path)
        os.unlink(out_path)


# ---------------------------------
