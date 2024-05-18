import json

from ResultProcessors import ResultProcessors
from stats import avg, count

BENCHMARK_TO_PROCESS_MAPPING = {
    'glmark2': "glmark2",
    'namd': "./namd2",
    'pytorch': "python3",
}


class GpuUtilizzationExtractorBase(ResultProcessors):
    def __init__(self):
        self.groups: dict[str, GpuUtilStats] = {}

    def process(self, gpu_util_json_file_name, content) -> None:
        parsed = json.loads(content)
        app_name = self.extract_benchmark_app_name(gpu_util_json_file_name)
        util_info = self.get_utilization_dict_info_from_process(app_name, parsed)
        if app_name not in self.groups:
            self.groups[app_name] = GpuUtilStats()
        self.groups[app_name].sum += util_info['gpu-sum']
        self.groups[app_name].count += util_info['count']

    def groups_to_values_mapping(self) -> dict[str, list[float]]:
        return self.groups
        # return {
        #     key: [value] for key,value in self.groups.items()
        # }

    def stats_to_consider(self) -> list[tuple[str, callable]]:
        return [('Average', avg), ('Count', count)]

    def extract_benchmark_app_name(self, file_name):
        file_name = file_name.lower()
        for benchmark_app in BENCHMARK_TO_PROCESS_MAPPING.keys():
            if benchmark_app in file_name:
                return benchmark_app
        assert False


    def get_utilization_dict_info_from_process(self, benchmark_app_name, parsed):
        benchmark_app_name = benchmark_app_name.lower()
        assert benchmark_app_name in ('glmark2', 'namd', 'pytorch')
        process_name = BENCHMARK_TO_PROCESS_MAPPING[benchmark_app_name]

        if process_name in parsed:
            return parsed[process_name]
        if "null" in parsed:
            return parsed["null"]
        assert False



class GpuUtilStats:
    def __init__(self, sum=0, count=0):
        self.count = count
        self.sum = sum

    def __add__(self, other):
        return GpuUtilStats(self.sum + other.sum, self.count + other.count)

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


def groups_to_values_mapping(self) -> dict[str, list[float]]:
        raise NotImplementedError



class GpuUtilizzationExtractor:
    def __init__(self, base: GpuUtilizzationExtractorBase, benchmark_app_name):
        self.benchmark_app_name = benchmark_app_name
        self.base = base

    def process(self, content) -> None:
        self.base.process(self.benchmark_app_name, content)
