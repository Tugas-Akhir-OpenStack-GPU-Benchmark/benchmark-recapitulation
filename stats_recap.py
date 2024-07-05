from __future__ import annotations
from typing import Callable

import pandas as pd

import namd_extractor
from ResultProcessors import ResultProcessors
from constants import openstack_service_col, benchmark_app_col, group_col, stat_name_col, value_col
from glmark2_extractor import MultiresolutionGlmark2ResultProcessor
from gpu_utilization_extractor import GpuUtilizzationExtractor, GpuUtilizzationExtractorBase
from pytorch_extractor import PytorchResultProcessor
from stats import *
from utils import convert_to_openstack_name, convert_to_openstack_latex_name


class StatsRecap:
    def __init__(self, array_of_values: list[float]):
        self.array_of_values = array_of_values
        self.stats_calculation_result: dict[str, float] = {}
        self.stats_to_be_calculated: list[tuple[str, Callable]] = None

    def calculate_stats(self, stats_to_consider: list[tuple[str, Callable]], additional_argument):
        self.stats_to_be_calculated = stats_to_consider
        for stat_name, stat_func in stats_to_consider:
            result = stat_func(self.array_of_values, additional_argument=additional_argument)
            self.stats_calculation_result[stat_name] = result


class StatRecapPerBenchmarkApp:
    def __init__(self, higher_nominals_means_worse_performance=False):
        self.grouping_to_stats_recap_mapping: dict[str, StatsRecap] = {}
        self.higher_nominals_means_worse_performance = higher_nominals_means_worse_performance

    def add_group(self, group_name, stats_recap: StatsRecap):
        self.grouping_to_stats_recap_mapping[group_name] = stats_recap

    def get_group_stats(self, group_name: str):
        return self.grouping_to_stats_recap_mapping[group_name]


    def calculate_stats(self, resultProcessor: ResultProcessors, comparison: StatRecapPerBenchmarkApp):
        for group, values in resultProcessor.groups_to_values_mapping().items():
            stat_recap = StatsRecap(values)
            self.add_group(group, stat_recap)
            stat_recap.calculate_stats(resultProcessor.stats_to_consider(), comparison.get_group_stats(group).array_of_values)

    # Specific to particular stats
    def get_combined_average(self):
        stat_recaps = list(self.grouping_to_stats_recap_mapping.values())
        total_sum = 0
        total_count = 0
        for stat_recap in stat_recaps:
            count = stat_recap.stats_calculation_result['Count']
            total_sum += stat_recap.stats_calculation_result['Average'] * count
            total_count += count
        return  total_sum / total_count



class StatRecapPerOpenStackService:
    def __init__(self, openstack_service_name: str):
        # "openstack service" refers to individual files inside folder ./data
        self.openstack_service_name = openstack_service_name
        self.as_comparison = False

        self.glmark2_processor: MultiresolutionGlmark2ResultProcessor = None
        self.glmark2: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp(False)
        self.namd_processor: namd_extractor.NamdResultProcessor = None
        self.namd: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp(True)
        self.pytorch_processor: PytorchResultProcessor = None
        self.pytorch: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp(False)
        self.gpu_util_processor: GpuUtilizzationExtractorBase = None
        self.gpu_util: StatRecapPerBenchmarkApp = StatRecapPerBenchmarkApp(False)

    def as_dict(self) -> dict[str, StatRecapPerBenchmarkApp]:
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
                    for stat_name, stat_value_result in stat_recap.stats_calculation_result.items():
                        key = (benchmark_app, group, stat_name)
                        if key not in rows:
                            rows[key] = []
                        rows[key].append(stat_value_result)
        table = []
        for (benchmark_app, group, stat_name), list_of_values in rows.items():
            table.append([benchmark_app, group, stat_name] + list_of_values)
        return table

    @staticmethod
    def as_dataframe(all_openstack_service_stat_recap: dict[str, StatRecapPerOpenStackService]) -> pd.DataFrame:
        rows = {}
        ret = []
        stat_per_benchmark_app: StatRecapPerBenchmarkApp
        for openstack_name, openstack_service_stat_recap in all_openstack_service_stat_recap.items():
            for benchmark_app, stat_per_benchmark_app in openstack_service_stat_recap.as_dict().items():
                for group, stat_recap in stat_per_benchmark_app.grouping_to_stats_recap_mapping.items():
                    for stat_name, stat_value_result in stat_recap.stats_calculation_result.items():
                        ret.append([openstack_name, benchmark_app, group, stat_name, stat_value_result])
        return pd.DataFrame.from_records(ret, columns=[openstack_service_col, benchmark_app_col, group_col, stat_name_col, value_col])

    @staticmethod
    def as_latex_variables(all_openstack_service_stat_recap: list[StatRecapPerOpenStackService]) -> list[str]:
        ret = []

        stat_per_benchmark_app: StatRecapPerBenchmarkApp
        for openstack_service_stat_recap in all_openstack_service_stat_recap:
            openstack_service_name = extract_openstack_service_name(openstack_service_stat_recap.openstack_service_name)
            ret.append(f"% {'='*40} {openstack_service_stat_recap.openstack_service_name} {'='*40}")

            for benchmark_app, stat_per_benchmark_app in openstack_service_stat_recap.as_dict().items():
                comparison_ratios = []
                benchmark_app_latex_var_name = replace_forbidden_names(sanitize(benchmark_app).title())
                for group, stat_recap in stat_per_benchmark_app.grouping_to_stats_recap_mapping.items():
                    group_latex_var_name = replace_forbidden_names(group).title()

                    for stat_name, stat_func  in stat_recap.stats_to_be_calculated:
                        stat_value_result = stat_recap.stats_calculation_result[stat_name]
                        # Cannot have underscores
                        key = (f"{openstack_service_name}"
                               f"{benchmark_app_latex_var_name}"
                               f"{group_latex_var_name}"
                               f"{sanitize(stat_func.__name__).title()}")
                        ret.append(getLatexDeclaration(key, round(stat_value_result, 5)))
                    ret.append(add_array_to_latex(openstack_service_name, benchmark_app_latex_var_name, group_latex_var_name, stat_recap))
                    comparison_ratios.append(get_ratio_comparison(openstack_service_stat_recap,
                                                                  benchmark_app, all_openstack_service_stat_recap, group))
                    ret.append(getLatexDeclaration(f"{openstack_service_name}{benchmark_app_latex_var_name}{group_latex_var_name}AverageRatio",
                                                   as_percentage(comparison_ratios[-1])))
                    ret.append(getLatexDeclaration(f"{openstack_service_name}{benchmark_app_latex_var_name}{group_latex_var_name}DecrementRatio",
                                                   as_percentage(1 - comparison_ratios[-1])))
                ret.append(getLatexDeclaration(f"{openstack_service_name}{benchmark_app_latex_var_name}OverallAverageRatio",
                                               as_percentage(avg(comparison_ratios))))
                ret.append(getLatexDeclaration(f"{openstack_service_name}{benchmark_app_latex_var_name}OverallDecrementRatio",
                                               as_percentage(1 - avg(comparison_ratios))))
            ret.append("")
            ret.append("")
            ret.append("")
        return ret


def add_array_to_latex(openstack_service_name, benchmark_app_latex_var_name, group_latex_var_name, stat_recap):
    key = f"BOXPLOT{openstack_service_name}{benchmark_app_latex_var_name}{group_latex_var_name}Array"
    data = "data\\\\" + "\\\\".join(list(map(str, stat_recap.array_of_values))) + "\\\\"
    data = "\\addplot+[boxplot, fill, draw=black,] table[row sep=\\\\,y index=0] {"+data+"};"
    return getLatexDeclaration(key, data)



def get_ratio_comparison(current_openstack_stat_recap: StatRecapPerOpenStackService, benchmark_name: str,
                         all_openstack_service_stat_recap: list[StatRecapPerOpenStackService], group_name):
    comparison: StatRecapPerOpenStackService = next(filter(lambda x: x.as_comparison, all_openstack_service_stat_recap))

    comparison_benchmark_app = comparison.as_dict()[benchmark_name]
    average_of_comparison = (comparison_benchmark_app.grouping_to_stats_recap_mapping[group_name]
        .stats_calculation_result['Average'])
    current_benchmark_app = current_openstack_stat_recap.as_dict()[benchmark_name]
    average_of_current = (current_benchmark_app.grouping_to_stats_recap_mapping[group_name]
        .stats_calculation_result['Average'])
    if not current_benchmark_app.higher_nominals_means_worse_performance:
        return average_of_current / average_of_comparison
    return average_of_comparison / average_of_current

def as_percentage(ratio_float: float):
    return f"{round(ratio_float*100, 1)}"


def getLatexDeclaration(name: str, value):
    assert name.isalpha()
    return "\\var{\\" + name + "}{" + str(value) + "}"



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
    return sanitize(convert_to_openstack_latex_name(string))


