import argparse
import dmtest.fixture
import dmtest.test_register as test_register
import dmtest.thin.creation_tests as thin_creation


# -----------------------------------------
# 'list' command


def cmd_list(tests, args):
    for p in tests.paths(args.rx):
        print(f"    {p}")


# -----------------------------------------
# 'run' command


def cmd_run(tests, args):
    for p in tests.paths(args.rx):
        print("***************************************")
        print(f"Running '{p}'")

        fix = dmtest.fixture.Fixture()
        tests.run(p, fix)


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


# -----------------------------------------
# Main


def main():
    parser = command_line_parser()
    args = parser.parse_args()

    tests = test_register.TestRegister()
    thin_creation.register(tests)

    args.func(tests, args)


main()
