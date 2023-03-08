import argparse
import dmtest.bufio.bufio_tests as bufio
import dmtest.fixture
import dmtest.test_register as test_register
import dmtest.thin.creation_tests as thin_creation
import logging as log
import os


# -----------------------------------------
# 'list' command


def cmd_list(tests, args):
    for p in tests.paths(args.rx):
        print(f"    {p}")


# -----------------------------------------
# 'run' command


def cmd_run(tests, args):
    for p in tests.paths(args.rx):
        print(f"{p} ... ", end="", flush=True)
        log.info(f"Running '{p}'")

        fix = dmtest.fixture.Fixture()
        passed = True
        try:
            tests.run(p, fix)
        except Exception as e:
            passed = False
            log.error(f"Exception caught: {e}")

        if passed:
            print("PASS")
        else:
            print("FAIL")
            log.info(f"*** FAIL {p}")


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
