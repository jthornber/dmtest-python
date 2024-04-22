import dmtest.process as process

import configparser
import logging as log
import tempfile
import os

# ---------------------------------


def default_fio_config():
    config = configparser.ConfigParser()

    config["global"] = {
        "randrepeat": "1",
        "ioengine": "libaio",
        "bs": "4k",
        "ba": "4k",
        "size": "5G",
        "numjobs": "1",
        "direct": "1",
        "gtod_reduce": "1",
        "iodepth": "64",
        "runtime": "20",
    }

    config["mix"] = {
        "rw": "randrw",
        "timeout": "30",
    }

    return config


def run_fio(dev, fio_config):
    config_path = tempfile.mkstemp()[1]
    with open(config_path, "w") as cfg:
        fio_config.write(cfg, False)

    out_path = tempfile.mkstemp()[1]
    process.run(f"fio --filename={dev} --output={out_path} {config_path}")

    # read the contents of out_path and log it.
    with open(out_path, "r") as out_file:
        out_contents = out_file.read()
        log.info(out_contents)

    # unlink config_path and out_path
    os.unlink(config_path)
    os.unlink(out_path)


# ---------------------------------
