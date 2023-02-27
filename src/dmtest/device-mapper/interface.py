import re
import utils

from process import run


def create(name):
    run(f"dmsetup create {name} --notable")


def load(name, table):
    with utils.temp_file() as (f, path):
        f.write(table.to_string())
        f.flush()
        run(f"dmsetup load {name} {path}")


def load_ro(name, table):
    with utils.temp_file() as (f, path):
        f.write(table.to_string())
        f.flush()
        run(f"dmsetup load --readonly {name} {path}")


def suspend(name):
    run(f"dmsetup suspend {name}")


def resume(name):
    run(f'dmsetup resume {name}')


def remove(name):
    def _remove(name):
        run(f'dmsetup remove {name}')

    utils.retry_if_fails(_remove, retry_delay=5)


def message(name, sector, *args):
    run(f"dmsetup message {name} {sector} {' '.join(args)}")


def status(name, *args):
    run(f"dmsetup status {' '.join(args)} {name}")


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
