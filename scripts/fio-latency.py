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


def extract_bins(fio_data, io_dir, merge_count):
    bins = fio_data["jobs"][0][io_dir]["clat_ns"]["bins"]
    bins = {int(k) / 1000000: v for k, v in bins.items()}
    return merge_bins(bins, merge_count)


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


def get_max_bin_count(results):
    counts = []
    for fio in results:
        counts += list(fio.read_bins.values())
        counts += list(fio.write_bins.values())

    return safe_log(max(10, max(counts)))


def get_max_bw(results):
    max_bws = []
    for fio in results:
        max_bws.append(fio.read_bw)
        max_bws.append(fio.write_bw)

    return max(max_bws)


def get_max_iops(results):
    max_iops = []
    for fio in results:
        max_iops.append(fio.read_iops)
        max_iops.append(fio.write_iops)

    return max(max_iops)


def bar(char, width, count, max_count):
    fraction = count / max_count
    nr_chars = int(float(width) * fraction)
    return char * nr_chars


def plot_latency(results, terminal_width):
    max_latency = math.ceil(get_max_latency(results))
    max_counts = math.ceil(get_max_bin_count(results))

    for fio in results:
        print(f"{fio.name}")

        fig = plotille.Figure()
        fig.width = terminal_width
        fig.height = 15
        fig.color_mode = "byte"
        fig.set_x_limits(min_=0, max_=max_latency)
        fig.set_y_limits(min_=1, max_=max_counts)
        fig.x_label = "Latency (ms)"
        fig.y_label = "ln2(count)"

        plot_bins(fig, fio.read_bins, "read")
        plot_bins(fig, fio.write_bins, "write")
        print(fig.show(legend=False))
        print("")


def plot_bandwidth(results, terminal_width):
    max_bw = get_max_bw(results)
    print("Bandwidth")
    for fio in results:
        print(f"  {fio.name}: {int(fio.read_bw)}/{int(fio.write_bw)}")
        print(f"    r: {bar('-', terminal_width, fio.read_bw, max_bw)}")
        print(f"    w: {bar('-', terminal_width, fio.write_bw, max_bw)}")
    print("")


def plot_iops(results, terminal_width):
    max_iops = get_max_iops(results)
    print("IOPS")
    for fio in results:
        print(f"  {fio.name}: {int(fio.read_iops)}/{int(fio.write_iops)}")
        print(f"    r: {bar('-', terminal_width, fio.read_iops, max_iops)}")
        print(f"    w: {bar('-', terminal_width, fio.write_iops, max_iops)}")
    print("")


def main():
    terminal_width = 160

    parser = argparse.ArgumentParser(description="Read and display file contents.")
    parser.add_argument("filenames", nargs="+", help="List of filenames to read")
    args = parser.parse_args()

    results = read_results(args.filenames)
    plot_bandwidth(results, terminal_width)
    plot_iops(results, terminal_width)
    plot_latency(results, terminal_width)


if __name__ == "__main__":
    main()
