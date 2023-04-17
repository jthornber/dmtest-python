import argparse
import dmtest.bufio.bufio_tests as bufio
import dmtest.db as db
import dmtest.fixture
import dmtest.process as process
import dmtest.test_register as test_register
import dmtest.thin.creation_tests as thin_creation
import dmtest.thin.deletion_tests as thin_deletion
import io
import itertools
import logging as log
import os
import sys
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
                strs.append(new.ljust(50, " ") + "\n")
            depth += 1
        self._previous = components
        return "".join(strs)[:-1]


# -----------------------------------------
# 'result set' should come from command line
# or environment.


def get_result_set(args):
    if args.result_set:
        return str(args.result_set)

    rs = os.environ.get("DMTEST_RESULT_SET", None)
    if rs:
        return str(rs)

    print(
        """
Missing result set.

This can be specified either on the command line:
    --result-set device-mapper2

or by setting an environment variable:
    export DMTEST_RESULT_SET=device-mapper2

The result set can be any string that is meaningful to you,
eg 'bufio-rewrite'.
    """,
        file=sys.stderr,
    )
    sys.exit(1)


# -----------------------------------------
# 'result-sets' command


def cmd_result_sets(tests, args, results: db.TestResults):
    for rs in results.get_result_sets():
        print(f"    {rs}")


# -----------------------------------------
# 'result-set-delete' command


def cmd_result_set_delete(tests, args, results: db.TestResult):
    try:
        results.delete_result_set(args.result_set)
    except db.NoSuchResultSet:
        print(f"No such result set '{args.result_set}'", file=sys.stderr)


# -----------------------------------------
# 'list' command


def cmd_list(tests, args, results: db.TestResults):
    result_set = get_result_set(args)
    paths = sorted(tests.paths(args.rx))
    formatter = TreeFormatter()

    for p in paths:
        result = results.get_test_result(p, result_set)
        print(f"{formatter.tree_line(p)}", end="")
        if result:
            print(f"{result.pass_fail} [{result.duration:.2f}s]")
        else:
            print("-")


# -----------------------------------------
# 'log' command


def cmd_log(tests, args, results: db.TestResults):
    result_set = get_result_set(args)
    paths = sorted(tests.paths(args.rx))

    for p in paths:
        result = results.get_test_result(p, result_set)
        if result:
            if len(paths) > 1:
                print(f"*** LOG FOR {p}, {len(result.log)} ***")
            print(result.log)
        else:
            print(f"*** NO LOG FOR {p}")


# -----------------------------------------
# 'run' command


def cmd_run(tests, args, results: db.TestResults):
    result_set = get_result_set(args)

    # select tests
    paths = sorted(tests.paths(args.rx))
    formatter = TreeFormatter()

    # Set up the logging
    buffer = io.StringIO()
    log.basicConfig(
        level=log.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=buffer,
    )

    for p in paths:
        buffer.seek(0)
        buffer.truncate()

        print(f"{formatter.tree_line(p)}", end="", flush=True)
        log.info(f"Running '{p}'")

        fix = dmtest.fixture.Fixture()
        passed = True
        missing_dep = None
        start = time.time()
        try:
            tests.run(p, fix)

        except test_register.MissingTestDep as e:
            missing_dep = e

        except Exception as e:
            passed = False
            log.error(f"Exception caught: {e}")
        elapsed = time.time() - start

        pass_str = None
        if missing_dep:
            print(f"MISSING_DEP [{missing_dep}]")
            pass_str = "MISSING_DEP"
        elif passed:
            print(f"PASS [{elapsed:.2f}s]")
            pass_str = "PASS"
        else:
            print("FAIL")
            pass_str = "FAIL"

        test_log = buffer.getvalue()
        result = db.TestResult(p, pass_str, test_log, result_set, elapsed)
        results.insert_test_result(result)


# -----------------------------------------
# 'health' command


def which(executable):
    (return_code, stdout, stderr) = process.run(f"which {executable}")
    if return_code == 0:
        return stdout
    else:
        return "-"


def cmd_health(tests, args, results):
    tools = ["dd", "blktrace", "blockdev", "dmsetup", "thin_check"]
    for t in tools:
        print(f"{(t + ' ').ljust(40, '.')} {which(t)}")


# -----------------------------------------
# Command line parser


def arg_filter(p):
    p.add_argument(
        "--rx",
        metavar="PATTERN",
        type=str,
        nargs="*",
        help="select tests that match the given pattern",
    )


def arg_result_set(p):
    p.add_argument(
        "--result-set",
        metavar="RESULT_SET",
        type=str,
        help="Specify a nickname for the kernel you are testing",
    )


def command_line_parser():
    parser = argparse.ArgumentParser(
        prog="dmtest", description="run device-mapper tests"
    )
    subparsers = parser.add_subparsers()

    result_sets_p = subparsers.add_parser("result-sets", help="list result sets")
    result_sets_p.set_defaults(func=cmd_result_sets)

    result_set_delete_p = subparsers.add_parser(
        "result-set-delete", help="list result sets"
    )
    result_set_delete_p.set_defaults(func=cmd_result_set_delete)
    result_set_delete_p.add_argument("result_set", help="The result set to delete")

    list_p = subparsers.add_parser("list", help="list tests")
    list_p.set_defaults(func=cmd_list)
    arg_filter(list_p)
    arg_result_set(list_p)

    log_p = subparsers.add_parser("log", help="list tests")
    log_p.set_defaults(func=cmd_log)
    arg_filter(log_p)
    arg_result_set(log_p)

    run_p = subparsers.add_parser("run", help="run tests")
    run_p.set_defaults(func=cmd_run)
    arg_filter(run_p)
    arg_result_set(run_p)

    health_p = subparsers.add_parser(
        "health", help="check required tools are installed"
    )
    health_p.set_defaults(func=cmd_health)

    return parser


# -----------------------------------------
# Main


def main():
    parser = command_line_parser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(0)

    tests = test_register.TestRegister()
    thin_creation.register(tests)
    thin_deletion.register(tests)
    bufio.register(tests)

    try:
        with db.TestResults("test_results.db") as results:
            args.func(tests, args, results)
    except BrokenPipeError:
        os._exit(0)


if __name__ == "__main__":
    main()
