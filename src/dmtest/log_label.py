import logging as log
import re

# -----------------------------------------


class LabelStack(object):
    def __init__(self):
        self.labels = []

    def push(self, label):
        self.labels.append(label)

    def pop(self):
        self.labels.pop()

    def compound_label(self):
        cl = ""
        first = True
        for label in self.labels:
            if first:
                first = False
            else:
                cl += "/"

            cl += label

        return cl


LABEL_STACK = LabelStack()

# -----------------------------------------


class LabelContext(object):
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        LABEL_STACK.push(self.label)
        log.info(f"<<< BEGIN {LABEL_STACK.compound_label()} >>>")

    def __exit__(self, _type, _value, _traceback):
        log.info(f"<<< END {LABEL_STACK.compound_label()} >>>")
        LABEL_STACK.pop()


def log_label(label):
    return LabelContext(label)


# -----------------------------------------


def line_matches(lines, rx):
    # Regular expression to match lines containing 'test'
    pattern = re.compile(rx)

    # List to hold the indices of matching lines
    matches = []

    # Iterate over the lines with their indices
    for index, line in enumerate(lines):
        if pattern.search(line):  # Check if the line matches the pattern
            matches.append(index)  # Store the index

    return matches


def filter_log(log, label_rx):
    # Split the string into lines
    lines = log.splitlines()

    begins = line_matches(lines, f"<<< BEGIN {label_rx} >>>")
    ends = line_matches(lines, f"<<< END {label_rx} >>>")

    nr_matches = min(len(begins), len(ends))
    for m in range(nr_matches):
        for i in range(begins[m] + 1, ends[m]):
            line = lines[i]
            if line:
                print(lines[i])


# -----------------------------------------
