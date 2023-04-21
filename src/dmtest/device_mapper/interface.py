import dmtest.dependency_tracker as dep
import dmtest.utils as utils
import logging as log
import re

from dmtest.process import run


def register_targets(table):
    for t in table:
        dep.add_target(t.type)


def create(name):
    run(f"dmsetup create {name} --notable")


def load(name, table):
    register_targets(table)
    with utils.TempFile() as tf:
        f = tf.file
        lines = table.table_lines()
        log.info(f"table file '{tf.path}':\n{lines}")
        f.write(lines)
        f.flush()
        run(f"dmsetup load {name} {tf.path}")


def load_ro(name, table):
    register_targets(table)
    with utils.TempFile() as tf:
        f = tf.file()
        lines = table.table_lines()
        log.info(f"table file '{tf.path}':\n{lines}")
        f.write(lines)
        f.flush()
        run(f"dmsetup load --readonly {name} {tf.path()}")


def suspend(name):
    run(f"dmsetup suspend {name}")


def resume(name):
    run(f"dmsetup resume {name}")


def remove(name):
    def _remove():
        run(f"dmsetup remove {name}")

    # udev is slow and sometimes prevents remove from completing
    utils.retry_if_fails(_remove, retry_delay=1)


def message(name, sector, *args):
    run(f"dmsetup message {name} {sector} {' '.join(args)}")


def status(name, *args):
    (_, stdout, _) = run(f"dmsetup status {' '.join(args)} {name}")
    return stdout


def table(name):
    run(f"dmsetup table {name}")


def info(name):
    run(f"dmsetup info {name}")


def parse_event_nr(txt):
    m = re.search(r"Event number:[ \t]*([0-9+])", txt)
    if not m:
        raise ValueError("Output does not contain an event number")

    return int(m.group(1))


def wait(name, event_nr):
    (_, stdout, _) = run(f"dmsetup wait -v {name} {event_nr}")
    return parse_event_nr(stdout)
