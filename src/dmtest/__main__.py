import argparse
import dmtest.bufio.bufio_tests as bufio
import dmtest.fixture
import dmtest.process as process
import dmtest.test_register as test_register
import dmtest.thin.creation_tests as thin_creation
import itertools
import logging as log
import os
import time


class TreeFormatter:
    def __init__(self):
        self._previous = []
        self._indent = "  "

    def tree_line(self, path):
        components = [c for c in path.split("/") if c.strip()]
        strs = []
        depth = 0
        for old, new in itertools.zip_longest(
            self._previous, components, fillvalue=None
        ):
            if old != new:
                strs.append(self._indent * depth)
                strs.append(new.ljust(60, " ") + "\n")
            depth += 1
        self._previous = components
        return "".join(strs)[:-1]


# -----------------------------------------
# 'list' command


def cmd_list(tests, args):
    paths = sorted(tests.paths(args.rx))
    formatter = TreeFormatter()

    for p in paths:
        print(f"{formatter.tree_line(p)}")


# -----------------------------------------
# 'run' command


def cmd_run(tests, args):
    # select tests
    paths = sorted(tests.paths(args.rx))
    formatter = TreeFormatter()

    for p in paths:
        print(f"{formatter.tree_line(p)}", end="", flush=True)
        log.info(f"Running '{p}'")

        fix = dmtest.fixture.Fixture()
        passed = True
        start = time.time()
        try:
            tests.run(p, fix)

        except Exception as e:
            passed = False
            log.error(f"Exception caught: {e}")
            raise
        elapsed = time.time() - start

        if passed:
            print(f"PASS [{elapsed:.2f}s]")
        else:
            print("FAIL")
            log.info(f"*** FAIL {p}")


# -----------------------------------------
# 'health' command


def which(executable):
    (return_code, stdout, stderr) = process.run(f"which {executable}")
    if return_code == 0:
        return stdout
    else:
        return "-"


def cmd_health(tests, args):
    tools = ["dd", "blktrace", "blockdev", "dmsetup", "thin_check"]
    for t in tools:
        print(f"{(t + ' ').ljust(40, '.')} {which(t)}")


# -----------------------------------------
# Command line parser


def add_filter(p):
    p.add_argument(
        "--rx",
        metavar="PATTERN",
        type=str,
        nargs="*",
        help="select tests that match the given pattern",
    )


def command_line_parser():
    parser = argparse.ArgumentParser(
        prog="dmtest", description="run device-mapper tests"
    )
    subparsers = parser.add_subparsers()

    list_p = subparsers.add_parser("list", help="list tests")
    add_filter(list_p)
    list_p.set_defaults(func=cmd_list)

    run_p = subparsers.add_parser("run", help="run tests")
    add_filter(run_p)
    run_p.set_defaults(func=cmd_run)

    health_p = subparsers.add_parser(
        "health", help="check required tools are installed"
    )
    health_p.set_defaults(func=cmd_health)

    return parser


def setup_log():
    logfile = "dmtest.log"
    os.remove(logfile)
    log.basicConfig(
        filename="dmtest.log",
        format="%(asctime)s %(levelname)s %(message)s",
        level=log.INFO,
    )


# -----------------------------------------
# Main


def main():
    setup_log()

    parser = command_line_parser()
    args = parser.parse_args()

    tests = test_register.TestRegister()
    thin_creation.register(tests)
    bufio.register(tests)

    args.func(tests, args)


main()
