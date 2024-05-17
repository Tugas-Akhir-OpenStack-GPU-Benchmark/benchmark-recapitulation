from __future__ import annotations
from typing import Callable

import namd_extractor
from ResultProcessors import ResultProcessors
from glmark2_extractor import MultiresolutionGlmark2ResultProcessor
from gpu_utilization_extractor import GpuUtilizzationExtractor, GpuUtilizzationExtractorBase
from pytorch_extractor import PytorchResultProcessor
from stats import *






class StatsRecap:
    def __init__(self, array_of_values: list[float]):
        self.array_of_values = array_of_values
        self.stats: dict[str, float] = {}
        self.calculated_stats: list[tuple[str, Callable]] = None

    def calculate_stats(self, stats_to_consider: list[tuple[str, Callable]], additional_argument):
        self.calculated_stats = stats_to_consider
        for stat_name, stat_func in stats_to_consider:
            result = stat_func(self.array_of_values, additional_argument=additional_argument)
            self.stats[stat_name] = result


class StatRecapPerBenchmarkApp:
    def __init__(self):
        self.grouping_to_stats_recap_mapping: dict[str, StatsRecap] = {}

    def add_group(self, group_name, stats_recap: StatsRecap):
        self.grouping_to_stats_recap_mapping[group_name] = stats_recap

    def get_group_stats(self, group_name: str):
        return self.grouping_to_stats_recap_mapping[group_name]


    def calculate_stats(self, resultProcessor: ResultProcessors, comparison: StatRecapPerBenchmarkApp):
        for group, values in resultProcessor.groups_to_values_mapping().items():
            stat_recap = StatsRecap(values)
            self.add_group(group, stat_recap)
            stat_recap.calculate_stats(resultProcessor.stats_to_consider(), additional_argument=comparison.get_group_stats(group).array_of_values)





class StatRecapPerOpenStackService:
    def __init__(self, openstack_service_name: str):
        # "openstack service" refers to individual files inside folder ./data
        self.openstack_service_name = openstack_service_name


        self.glmark2_processor: MultiresolutionGlmark2ResultProcessor = None
        self.glmark2: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp()
        self.namd_processor: namd_extractor.NamdResultProcessor = None
        self.namd: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp()
        self.pytorch_processor: PytorchResultProcessor = None
        self.pytorch: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp()
        self.gpu_util_processor: GpuUtilizzationExtractorBase = None
        self.gpu_util: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp()

    def as_dict(self):
        return {
            'Glmark2': self.glmark2,
            'NAMD': self.namd,
            'PyTorch': self.pytorch,
            'GpuUtil': self.gpu_util,
        }


    def calculate_benchmark(self, comparison: StatRecapPerOpenStackService):
        self.glmark2.calculate_stats(self.glmark2_processor, comparison.glmark2)
        self.namd.calculate_stats(self.namd_processor, comparison.namd)
        self.pytorch.calculate_stats(self.pytorch_processor, comparison.pytorch)
        self.gpu_util.calculate_stats(self.gpu_util_processor, comparison.gpu_util)

    @staticmethod
    def as_table(all_openstack_service_stat_recap: list[StatRecapPerOpenStackService]):
        rows = {}
        stat_per_benchmark_app: StatRecapPerBenchmarkApp
        for openstack_service_stat_recap in all_openstack_service_stat_recap:
            for benchmark_app, stat_per_benchmark_app in openstack_service_stat_recap.as_dict().items():
                for group, stat_recap in stat_per_benchmark_app.grouping_to_stats_recap_mapping.items():
                    for stat_name, stat_value_result in stat_recap.stats.items():
                        key = (benchmark_app, group, stat_name)
                        if key not in rows:
                            rows[key] = []
                        rows[key].append(stat_value_result)
        table = []
        for (benchmark_app, group, stat_name), list_of_values in rows.items():
            table.append([benchmark_app, group, stat_name] + list_of_values)
        return table

    @staticmethod
    def as_latex_variables(all_openstack_service_stat_recap: list[StatRecapPerOpenStackService]) -> list[str]:
        ret = []

        stat_per_benchmark_app: StatRecapPerBenchmarkApp
        for openstack_service_stat_recap in all_openstack_service_stat_recap:
            openstack_service_name = extract_openstack_service_name(openstack_service_stat_recap.openstack_service_name)

            ret.append(f"% {'='*40} {openstack_service_stat_recap.openstack_service_name} {'='*40}")

            for benchmark_app, stat_per_benchmark_app in openstack_service_stat_recap.as_dict().items():
                for group, stat_recap in stat_per_benchmark_app.grouping_to_stats_recap_mapping.items():
                    for stat_name, stat_func  in stat_recap.calculated_stats:
                        stat_value_result = stat_recap.stats[stat_name]
                        # Cannot have underscores
                        key = (f"{openstack_service_name}"
                               f"{replace_forbidden_names(sanitize(benchmark_app).title())}"
                               f"{replace_forbidden_names(group).title()}"
                               f"{sanitize(stat_func.__name__).title()}")
                        ret.append(
                            "\\var{\\"+ key +"}{"+ str(round(stat_value_result, 5)) +"}"
                        )
                    key = (f"BOXPLOT{openstack_service_name}"
                                 f"{replace_forbidden_names(sanitize(benchmark_app).title())}"
                                 f"{replace_forbidden_names(group).title()}Array")
                    data = "data\\\\" + "\\\\".join(list(map(str, stat_recap.array_of_values))) + "\\\\"
                    data = "\\addplot+[boxplot, fill, draw=black,] table[row sep=\\\\,y index=0] {"+data+"};"
                    ret.append(f"\\var{{\\{key}}}{{{data}}}")
            ret.append("")
            ret.append("")
            ret.append("")
        return ret



def replace_forbidden_names(forbidden_name):
    names = {
        'Glmark2'.lower(): 'Glmark',
        'ResNet-50'.lower(): 'ResnetFivety',
        'ResNet-152'.lower(): 'ResnetOFT',
        'Efficientnet_v2_l'.lower(): 'Efnet',
        '1920x1080': 'LaptopBig',
        '1366x768': 'LaptopSmall',
        '360x800': 'Phone',
        '192x108': 'Smallest'
    }
    return names.get(forbidden_name.lower(), forbidden_name)


def sanitize(string):
    forbidden_characters = ['-', ',', '.', '_']
    for char in forbidden_characters:
        string = string.replace(char, '')
    return string


def extract_openstack_service_name(string: str):
    string = string.lower()
    values_to_find = ['nova', 'zun', 'ironic', 'physical', 'direct']
    for value in values_to_find:
        if value in string:
            return value
    return sanitize(string)


