import statistics

def Avg(arr):
    if not arr:
        return ''
    return sum(arr) / len(arr)


def Stdev(arr):
    return statistics.stdev(arr)


def Count(arr):
    return len(arr)


stat_functions = [Avg, Stdev, Count]