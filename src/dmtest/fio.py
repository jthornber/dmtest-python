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
        "iodepth": "64",
        "runtime": "20",
    }

    config["mix"] = {
        "rw": "randrw",
        "timeout": "30",
    }

    return config


def log_contents(banner, path):
    with open(path, "r") as file:
        log.info(f"{banner}:\n{file.read()}")


def run_fio(dev, fio_config):
    config_path = tempfile.mkstemp()[1]
    with open(config_path, "w") as cfg:
        fio_config.write(cfg, False)
    log_contents("fio config file", config_path)

    out_path = tempfile.mkstemp()[1]
    process.run(
        f"fio --filename={dev} --output={out_path} --output-format=json+ {config_path}"
    )

    log_contents("fio output", out_path)

    # unlink config_path and out_path
    os.unlink(config_path)
    os.unlink(out_path)


# ---------------------------------
