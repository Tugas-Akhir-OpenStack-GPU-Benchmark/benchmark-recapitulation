from __future__ import annotations
import json

import pandas as pd
from scipy.stats import ttest_ind_from_stats

from ResultProcessors import ResultProcessors
from stats import avg, count

BENCHMARK_TO_PROCESS_MAPPING = {
    'Glmark2': "glmark2",
    'NAMD': "./namd2",
    'PyTorch': "python3",
}

TWO_SIDED_P_VALUE_EQUAL = "≠ physical; p-value"

class GpuUtilizzationExtractorBase(ResultProcessors):
    def __init__(self):
        self.groups: dict[str, GpuUtilStats] = {}

    def process(self, gpu_util_json_file_name, content) -> None:
        parsed = json.loads(content)
        app_name = self.extract_benchmark_app_name(gpu_util_json_file_name)
        util_info = self.get_utilization_dict_info_from_process(app_name, parsed)
        if app_name not in self.groups:
            self.groups[app_name] = GpuUtilStats()
        currentData = GpuUtilStats(util_info['gpu-sum'], util_info['count'], util_info['gpu-variance'])
        self.groups[app_name] = self.groups[app_name] + currentData

    def groups_to_values_mapping(self) -> dict[str, GpuUtilStats]:
        return self.groups

    def stats_to_consider(self) -> list[tuple[str, callable]]:
        return [('Average', avg), ('Count', count), ('Stdev', stdevForGpuutils), (TWO_SIDED_P_VALUE_EQUAL, ttest_two_tail)]

    def extract_benchmark_app_name(self, file_name):
        file_name = file_name.lower()
        for benchmark_app in BENCHMARK_TO_PROCESS_MAPPING.keys():
            if benchmark_app.lower() in file_name:
                return benchmark_app
        assert False


    def get_utilization_dict_info_from_process(self, benchmark_app_name, parsed):
        benchmark_app_name = benchmark_app_name
        assert benchmark_app_name in BENCHMARK_TO_PROCESS_MAPPING.keys()
        process_name = BENCHMARK_TO_PROCESS_MAPPING[benchmark_app_name]

        if process_name in parsed:
            return parsed[process_name]
        if "null" in parsed:
            return parsed["null"]
        assert False

    def as_dataframe(self) -> pd.DataFrame:
        data = []
        for benchmark_app, gpu_util in self.groups_to_values_mapping().items():
            for value in gpu_util:
                data.append({
                    'benchmark': benchmark_app,
                    'gpu-util': gpu_util.sum / gpu_util.count,
                    'count': gpu_util.count,
                    'stdev': gpu_util.stdev,
                })
        return pd.DataFrame(data)


class GpuUtilStats:
    def __init__(self, sum=0, count=0, variance=0):
        self.count = count
        self.sum = sum
        self.variance = variance

    @property
    def average(self):
        if self.count == 0:
            return 9999999999999
        return self.sum / self.count

    @property
    def stdev(self):
        return self.variance**.5

    def __add__(self, other):
        return GpuUtilStats(self.sum + other.sum, self.count + other.count, calculateVariance(self, other))

    def __radd__(self, other):
        if isinstance(other, int):
            return self
        return self + other

    def __rtruediv__ (self, other):
        return self

    def __truediv__ (self, other):
        return self

    def __len__(self):
        return self.count

    def __float__(self):
        return round(self.sum / self.count, 2)

    def __repr__(self):
        return f"{self.__float__()}"

    def __iter__(self):
        return iter([self])

    def __round__(self, ndigits=None):
        return round(self.__float__(), ndigits=min(2,ndigits or 100))


def calculateVariance(stats1: GpuUtilStats, stats2: GpuUtilStats):
    add1 = (stats1.count - 1) * stats1.variance
    add2 = (stats2.count - 1) * stats2.variance
    divisor = stats1.count + stats2.count - 1
    firstNumber = (add1 + add2) / divisor

    numerator = stats1.count*stats2.count * (stats1.average - stats2.average)**2
    denominator = (stats1.count + stats2.count) * (stats1.count + stats2.count - 1)
    secondNumber = numerator / denominator
    return firstNumber + secondNumber


def stdevForGpuutils(gpuUtil: GpuUtilStats, **_):
    return gpuUtil.stdev

def ttest_two_tail(gpuUtil: GpuUtilStats, additional_argument: GpuUtilStats):
    physical = additional_argument
    tstat, pvalue = ttest_ind_from_stats(gpuUtil.average, gpuUtil.stdev, gpuUtil.count, physical.average, physical.stdev, physical.count, alternative='two-sided')
    return pvalue

def groups_to_values_mapping(self) -> dict[str, list[float]]:
        raise NotImplementedError



class GpuUtilizzationExtractor:
    def __init__(self, base: GpuUtilizzationExtractorBase, benchmark_app_name):
        self.benchmark_app_name = benchmark_app_name
        self.base = base

    def process(self, content) -> None:
        self.base.process(self.benchmark_app_name, content)
