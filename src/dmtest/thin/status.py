import re


def _parse_usage(str):
    (used, total) = str.split("/")
    return (int(used), int(total))


def _parse_metadata_snap(str):
    if str == "-":
        return None
    else:
        return int(str)


def _parse_opts(h, toks):
    h["block-zeroing"] = True
    h["ignore-discard"] = False
    h["discard-passdown"] = True
    h["mode"] = "read-only"
    h["error-if-no-space"] = False

    for t in toks:
        if t == "skip_block_zeroing":
            h["block-zeroing"] = False

        elif t == "ignore_discard":
            h["ignore-discard"] = True

        elif t == "no_discard_passdown":
            h["discard-passdown"] = False

        elif t == "discard_passdown":
            h["discard-passdown"] = True

        elif t == "out_of_data_space":
            h["mode"] = "out-of-data-space"

        elif t == "ro":
            h["mode"] = "read-only"

        elif t == "rw":
            h["mode"] = "read-write"

        elif t == "error_if_no_space":
            h["error-if-no-space"] = True

        elif t == "queue_if_no_space":
            h["error-if-no-space"] = False

        else:
            raise ValueError(f"Bad pool option {t}")


def _parse_needs_check(str):
    return str == "needs_check"


def _parse_pool_status(str):
    tokens = re.split(r"\s+", str)[3:]

    h = {}
    h["transaction-id"] = int(tokens[0])

    (used, total) = _parse_usage(tokens[1])
    h["metadata-used"] = used
    h["metadata-total"] = total

    (used, total) = _parse_usage(tokens[2])
    h["data-used"] = used
    h["data-total"] = total

    h["metadata-snap"] = _parse_metadata_snap(tokens[3])

    _parse_opts(h, tokens[4:-2])

    h["needs-check"] = _parse_needs_check(tokens[-2])
    h["metadata-threshold"] = int(tokens[-1])

    return h


def pool_status(dev):
    return _parse_pool_status(dev.status())
