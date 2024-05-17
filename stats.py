from scipy.stats import ttest_ind
from scipy.stats._result_classes import TtestResult
import statistics

def avg(arr, **_):
    if not arr:
        return ''
    return float(sum(arr) / len(arr))


def stdev(arr, **_):
    return statistics.stdev(arr)


def count(arr, **_):
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

def t_test_new_api(arr1, arr2, alternative,):
    ret: TtestResult = ttest_ind(arr1, arr2, alternative=alternative)
    return ret.pvalue


class CustomNamedFunction:
    def __init__(self, name, func):
        self.name = name
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def get_name(self):
        return self.name



def p_value_equal(x, additional_argument):
    return t_test_new_api(additional_argument, x, 'two-sided')


def p_value_greater(x, additional_argument):
    return t_test_new_api(additional_argument, x, 'greater')


def p_value_less(x, additional_argument):
    return t_test_new_api(additional_argument, x, 'less')

def mininum(data, additional_argument):
    return min(data)
def maximum(data, additional_argument):
    return max(data)
def median(data, additional_argument):
    return statistics.median(data)

def lower_quantile(data, additional_argument):
    q1, med, q3 = statistics.quantiles(data, n=4)
    return q1


def upper_quantile(data, additional_argument):
    q1, med, q3 = statistics.quantiles(data, n=4)
    return q3

def lower_whisker(data, additional_argument):
    q1, med, q3 = statistics.quantiles(data, n=4)
    iqr = q3 - q1
    return q1 - 1.5*iqr




DEFAULT_STATS_TO_CONSIDER = [
    ('Average', avg),
    ('Stdev', stdev),
    ('Count', count),
    ('Min', mininum),
    ('Max', maximum),
    ('Q1', lower_quantile),
    ('Median', median),
    ('Q3', upper_quantile),

    ('= physical; p-value', p_value_equal),
]
GREATER_THAN_PHYSICAL = [
    ('> physical; p-value', p_value_greater),
]
LESS_THAN_PHYSICAL = [
    ('< physical; p-value', p_value_less),
]


def major_grouping_by_stat_name(table):
    table[1:] = sorted(table[1:], key=major_grouping_by_stat_name__sort_key)
    for columns in table:
        columns[0], columns[1], columns[2] = columns[0], columns[2], columns[1]

def major_grouping_by_stat_name__sort_key(item):
    benchmark_name, benchmark_group, function_name, *_ = item
    benchmark_name_ordering = ['Glmark2', 'NAMD', 'PyTorch']
    stat_function_name_ordering = list(map(lambda x: x[0], DEFAULT_STATS_TO_CONSIDER))
    stat_function_name_ordering.append(T_test_equal([]).get_name())
    try:
        return stat_function_name_ordering.index(function_name)
    except ValueError:
        return len(stat_function_name_ordering) + benchmark_name_ordering.index(benchmark_name)
