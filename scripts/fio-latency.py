import argparse
import json
import plotille
import os
import math


class FioResults:
    def __init__(
        self,
        name,
        read_bw,
        read_iops,
        read_bins,
        write_bw,
        write_iops,
        write_bins,
    ):
        self.name = name

        self.read_bw = read_bw
        self.read_iops = read_iops
        self.read_bins = read_bins

        self.write_bw = write_bw
        self.write_iops = write_iops
        self.write_bins = write_bins


def remove_up_to_first_brace(s):
    # Find the index of the first occurrence of '{'
    index = s.find("{")

    # If '{' is found, return the substring starting from this index
    if index != -1:
        return s[index:]
    else:
        # Return the original string or handle the case where '{' is not found
        return s


def read_fio_json(filename):
    with open(filename, "r") as file:
        contents = remove_up_to_first_brace(file.read())
        data = json.loads(contents)
    return data


def read_fio_results(filename):
    json = read_fio_json(filename)

    read_json = json["jobs"][0]["read"]
    write_json = json["jobs"][0]["write"]

    name = json["jobs"][0]["job options"]["name"]
    read_bw = read_json["bw_mean"]
    read_iops = read_json["iops_mean"]
    read_bins = extract_bins(json, "read", 2)

    write_bw = write_json["bw_mean"]
    write_iops = write_json["iops_mean"]
    write_bins = extract_bins(json, "write", 2)

    return FioResults(
        name, read_bw, read_iops, read_bins, write_bw, write_iops, write_bins
    )


# def extract_clat_data(fio_data, io_dir):
#     # Extracting latency data (in microseconds)
#     latencies = fio_data["jobs"][0][io_dir]["clat_ns"]["percentile"]
#     # Convert nanoseconds to milliseconds for better readability
#     latencies_ms = {k: v / 1000000 for k, v in latencies.items()}
#     return latencies_ms


def extract_bins(fio_data, io_dir, merge_count):
    bins = fio_data["jobs"][0][io_dir]["clat_ns"]["bins"]
    bins = {int(k) / 1000000: v for k, v in bins.items()}
    return merge_bins(bins, merge_count)


def plot_latency(fig, clat, legend):
    x = [float(k) for k in clat.keys()]
    y = [v for v in clat.values()]
    fig.plot(x, y, label=legend)


def merge_bins(bins_data, count):
    merged = {}
    keys = list(bins_data.keys())
    values = list(bins_data.values())
    for i in range(count, len(bins_data), count):
        merged[keys[i]] = sum(values[(i - count) : i])
    return merged


def safe_log(n):
    if n == 0:
        return 0

    return math.log2(float(n))


def plot_bins(fig, bins, legend):
    keys = list(bins.keys())
    values = list(bins.values())

    centers = []
    last = 0.0
    for k in keys:
        mid = last + k
        centers.append(mid)

    lg_values = []
    for v in values:
        lg_values.append(safe_log(v))

    fig.plot(centers, lg_values, label=legend)


def read_results(files):
    results = []
    for filename in files:
        results.append(read_fio_results(filename))

    return results


def get_max_latency(results):
    latencies = []

    for fio in results:
        latencies.append(list(fio.read_bins.keys())[-1])
        latencies.append(list(fio.write_bins.keys())[-1])

    return max(10, max(latencies))


def main():
    terminal_width = 160

    parser = argparse.ArgumentParser(description="Read and display file contents.")
    parser.add_argument("filenames", nargs="+", help="List of filenames to read")
    args = parser.parse_args()

    results = read_results(args.filenames)
    max_latency = get_max_latency(results)

    for fio in results:
        fig = plotille.Figure()
        fig.width = terminal_width
        fig.height = 15
        fig.color_mode = "byte"
        fig.set_x_limits(min_=0, max_=max_latency)
        fig.x_label = "Latency (ms)"
        fig.y_label = "ln2(count)"

        plot_bins(fig, fio.read_bins, "read")
        plot_bins(fig, fio.write_bins, "write")
        print(fig.show(legend=False))


if __name__ == "__main__":
    main()

    # FIXME: remove
    # fig = plotille.Figure()
    # fig.width = terminal_width
    # fig.height = 20
    # fig.set_x_limits(min_=0, max_=100)
    # fig.color_mode = "byte"
    # fig.y_label = "Latency (ms)"
    # fig.x_label = "Percentile"
    #
    # read_clat = extract_clat_data(fio_data, "read")
    # plot_latency(fig, read_clat, "read")
    #
    # write_clat = extract_clat_data(fio_data, "write")
    # plot_latency(fig, write_clat, "write")

    # print(fig.show(legend=True))
