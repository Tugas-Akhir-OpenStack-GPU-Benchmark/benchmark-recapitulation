import statistics

def average(arr):
    if not arr:
        return ''
    return sum(arr) / len(arr)


def stdev(arr):
    return statistics.stdev(arr)


def count(arr):
    return len(arr)


stat_functions = [average, stdev, count]