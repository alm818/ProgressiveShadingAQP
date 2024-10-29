import numpy as np
from datasketch import HyperLogLogPlusPlus

from ps.util.debug import deb

HLL_SIZE = 1000

class Stat:

    def __init__(self, columns, max_size):
        self.columns = columns[:]
        self.mins = np.full(len(columns), np.inf)
        self.maxs = np.full(len(columns), -np.inf)
        self.max_size = max_size
        if self.max_size > HLL_SIZE:
            self.ucounts = [HyperLogLogPlusPlus() for i in range(len(columns))]
        else:
            self.ucounts = [set() for i in range(len(columns))]
        self.n = 0
        self.means = np.zeros(len(columns))
        self.M2s = np.zeros(len(columns))

    def update(self, values):
        self.mins = np.minimum(self.mins, values)
        self.maxs = np.maximum(self.maxs, values)
        if self.max_size > HLL_SIZE:
            for i, value in enumerate(values):
                self.ucounts[i].update(str(value).encode('utf8'))
        else:
            for i, value in enumerate(values):
                self.ucounts[i].add(value)
        self.n += 1
        delta = values - self.means
        self.means += (delta / self.n)
        self.M2s += (delta * (values - self.means))

    def get_unique_counts(self):
        if self.max_size > HLL_SIZE:
            return np.array([ucount.count() for ucount in self.ucounts])
        else:
            return np.array([len(ucount) for ucount in self.ucounts])

def swap_pivot(arr, pivot):
    arr[0], arr[pivot] = arr[pivot], arr[0]
    return arr

def check(arr):
    return np.all(np.logical_and(arr >= 0, np.isfinite(arr)))

class Histogram:

    def __init__(self, stat):
        self.pivot = np.argmax(stat.M2s)
        self.columns = swap_pivot(stat.columns, self.pivot)
        self.mins = swap_pivot(stat.mins, self.pivot)
        self.maxs = swap_pivot(stat.maxs, self.pivot)

        bin_counts = stat.get_unique_counts()
        # pivot_sz = int(np.ceil(np.sqrt(bin_counts[self.pivot])))
        pivot_sz = int(np.ceil(np.cbrt(2 * bin_counts[self.pivot])))
        for i, v in enumerate(bin_counts):
            if v > 1:
                bin_counts[i] = np.ceil(np.cbrt(2 * bin_counts[i]))
        bin_counts[self.pivot] = pivot_sz
        bin_counts = swap_pivot(bin_counts, self.pivot).astype(int)

        self.one_counts = [np.zeros(sz, dtype=int) for sz in bin_counts]
        self.two_counts = [np.zeros((sz, pivot_sz), dtype=int) for sz in bin_counts[1:]]
        
        ranges = self.maxs - self.mins
        ranges[ranges == 0] = 1
        self.scales = bin_counts / ranges
        self.bin_counts_minus_one = bin_counts - 1
        self.n = stat.n

    def update(self, values):
        bin_indices = (swap_pivot(values, self.pivot) - self.mins) * self.scales
        bin_indices = np.clip(bin_indices, 0, self.bin_counts_minus_one).astype(int)
        for i, index in enumerate(bin_indices):
            self.one_counts[i][index] += 1
        for i in range(len(bin_indices) - 1):
            self.two_counts[i][bin_indices[i+1], bin_indices[0]] += 1