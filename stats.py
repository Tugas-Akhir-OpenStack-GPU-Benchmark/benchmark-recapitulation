from scipy.stats import ttest_ind
from scipy.stats._result_classes import TtestResult
import statistics

def avg(arr):
    if not arr:
        return ''
    return sum(arr) / len(arr)


def Stdev(arr):
    return statistics.stdev(arr)


def Count(arr):
    return len(arr)

def T_test_greater(physical_result: list):
    return CustomNamedFunction('> physical; p-value', lambda arr: t_test_wrappee(physical_result, arr, 'greater'))

def T_test_less(physical_result: list):
    return CustomNamedFunction('< physical; p-value', lambda arr: t_test_wrappee(physical_result, arr, 'less'))

def T_test_equal(physical_result: list):
    return CustomNamedFunction('= physical; p-value', lambda arr: t_test_wrappee(physical_result, arr, 'two-sided'))


def t_test_wrappee(comparison, arr, alternative):
    ret: TtestResult = ttest_ind(comparison, arr, alternative=alternative)
    return ret.pvalue

class CustomNamedFunction:
    def __init__(self, name, func):
        self.name = name
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def get_name(self):
        return self.name


stat_functions = [CustomNamedFunction('Average', avg),
                  CustomNamedFunction('Stdev', Stdev),
                  CustomNamedFunction('Count', Count)]


def major_grouping_by_stat_name(table):
    table[1:] = sorted(table[1:], key=major_grouping_by_stat_name__sort_key)
    for columns in table:
        columns[0], columns[1], columns[2] = columns[0], columns[2], columns[1]

def major_grouping_by_stat_name__sort_key(item):
    benchmark_name, benchmark_group, function_name, *_ = item
    benchmark_name_ordering = ['Glmark2', 'NAMD', 'PyTorch']
    stat_function_name_ordering = list(map(lambda x: x.get_name(), stat_functions))
    stat_function_name_ordering.append(T_test_equal([]).get_name())
    try:
        return stat_function_name_ordering.index(function_name)
    except ValueError:
        return len(stat_function_name_ordering) + benchmark_name_ordering.index(benchmark_name)
