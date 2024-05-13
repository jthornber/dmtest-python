import argparse
import json
import plotille
import os
import math

from plotille._cmaps import cmaps

# -------------------------


def counts_to_colours(counts):
    colour_map = cmaps["gray"]()
    max_count = float(max(counts))

    colours = []
    for c in counts:
        colours.append(colour_map(float(c) / max_count))
    return colours


def draw_heatmap(counts, width, height):
    assert len(counts) == width * height

    img = counts_to_colours(counts)

    cvs = plotille.Canvas(width, height, mode="rgb")
    cvs.image(img)
    print(cvs.plot())


def read_iolog(filenames):
    results = []
    for filename in filenames:
        with open(filename, "r") as file:
            line = file.readline()
            while line:
                components = line.split()
                if len(components) == 5:
                    results.append((components[2], int(components[3]), int(components[4])))
                line = file.readline()
    return results


def estimate_dev_len(iolog):
    dlen = 0
    for _, loc, len in iolog:
        dlen = max(dlen, loc + len)

    # FIXME: hack
    min_size = 5 * 1024 * 1024 * 1024
    return max(dlen, min_size)


def build_heatmaps(iolog, width, height):
    nr_bins = width * height
    reads = [0 for _ in range(nr_bins)]
    writes = [0 for _ in range(nr_bins)]

    bin_width = estimate_dev_len(iolog) / nr_bins

    for event, loc, _len in iolog:
        index = int(loc / bin_width)

        if event == "read":
            reads[index] += 1
        elif event == "write":
            writes[index] += 1
        else:
            raise ValueError("unknown event")

    return [reads, writes]


def hist_(title, locs, width):
    print(title)
    print(plotille.hist(locs, bins=20, width=width))


def loc_histogram(iolog, width):
    read_locs = []
    write_locs = []
    for event, loc, _ in iolog:
        if event == "read":
            read_locs.append(float(loc))
        elif event == "write":
            write_locs.append(float(loc))
        else:
            raise ValueError("unknown event type")

    hist_("read locations", read_locs, width)
    hist_("write locations", write_locs, width)


def len_histogram(iolog, width):
    read_lens = []
    write_lens = []
    for event, _, len in iolog:
        if event == "read":
            read_lens.append(float(len))
        elif event == "write":
            write_lens.append(float(len))
        else:
            raise ValueError("unknown event type")

    hist_("read lengths", read_lens, width)
    hist_("write lengths", write_lens, width)


def main():
    terminal_width = 160

    parser = argparse.ArgumentParser(description="Read and display file contents.")
    parser.add_argument("filenames", nargs="+", help="io-log to read")
    args = parser.parse_args()

    iolog = read_iolog(args.filenames)

    width = terminal_width
    height = 20
    reads, writes = build_heatmaps(iolog, width, height)
    print("reads")
    draw_heatmap(reads, width, height)

    print("writes")
    draw_heatmap(writes, width, height)

    # loc_histogram(iolog, terminal_width)
    len_histogram(iolog, terminal_width - 36)


if __name__ == "__main__":
    main()

# -------------------------
