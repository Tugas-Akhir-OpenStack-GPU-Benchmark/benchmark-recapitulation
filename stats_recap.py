from __future__ import annotations
from typing import Callable

import namd_extractor
from ResultProcessors import ResultProcessors
from glmark2_extractor import MultiresolutionGlmark2ResultProcessor
from pytorch_extractor import PytorchResultProcessor
from stats import *


DEFAULT_STATS_TO_CONSIDER = [
    ('Average', avg),
    ('Stdev', Stdev),
    ('Count', Count),
    ('= physical; p-value', lambda x, additional_argument: t_test_new_api(additional_argument, x, 'two-sided')),
]
GREATER_THAN_PHYSICAL = [
    ('> physical; p-value', lambda x, additional_argument: t_test_new_api(additional_argument, x, 'greater')),
]
LESS_THAN_PHYSICAL = [
    ('< physical; p-value', lambda x, additional_argument: t_test_new_api(additional_argument, x, 'less')),
]


class StatsRecap:
    def __init__(self, array_of_values: list[float]):
        self.array_of_values = array_of_values
        self.stats: dict[str, float] = {}

    def calculate_stats(self, stats_to_consider: list[tuple[str, Callable]], additional_argument):
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


    def calculate_stats(self, resultProcessor: ResultProcessors, stats_to_consider, comparison: StatRecapPerBenchmarkApp):
        for group, values in resultProcessor.groups_to_values_mapping().items():
            stat_recap = StatsRecap(values)
            self.add_group(group, stat_recap)
            stat_recap.calculate_stats(stats_to_consider, additional_argument=comparison.get_group_stats(group).array_of_values)





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

    def as_dict(self):
        return {
            'Glmark2': self.glmark2,
            'NAMD': self.namd,
            'PyTorch': self.pytorch,
        }


    def calculate_benchmark(self, comparison: StatRecapPerOpenStackService):
        self.glmark2.calculate_stats(self.glmark2_processor, DEFAULT_STATS_TO_CONSIDER + LESS_THAN_PHYSICAL, comparison.glmark2)
        self.namd.calculate_stats(self.namd_processor, DEFAULT_STATS_TO_CONSIDER + GREATER_THAN_PHYSICAL, comparison.namd)
        self.pytorch.calculate_stats(self.pytorch_processor, DEFAULT_STATS_TO_CONSIDER + LESS_THAN_PHYSICAL, comparison.pytorch)

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



