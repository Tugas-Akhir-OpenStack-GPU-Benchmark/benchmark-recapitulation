import itertools
from typing import Any


def transpose(two_d_arr, jagged_fill_value=None):
    return list(map(list, itertools.zip_longest(*two_d_arr, fillvalue=jagged_fill_value)))


def flatten_dict_of_list(dct_of_lst: dict[tuple, list]):
    new_dct = {}
    for key, values in dct_of_lst.items():
        for index, value in enumerate(values):
            new_dct[key + (index,)] = value
    return new_dct


def combine_dicts(dictionaries: dict, key_sorter=lambda x: x, jagged_default_value=ValueError("Jagged")):
    keys = set()
    for dct in dictionaries:
        keys |= dct.keys()
    keys = key_sorter(list(keys))

    ret: dict[str, list]
    ret = {key: [] for key in keys}
    for dictionary in dictionaries:
        for key in keys:
            value = dictionary.get(key, jagged_default_value)
            key_not_exists = (key not in dictionary)
            if key_not_exists and isinstance(jagged_default_value, Exception):
                raise jagged_default_value
            if key_not_exists and isinstance(jagged_default_value, Nothing):
                continue
            ret[key].append(value)
    return ret


def transpose_dict(dictionaries: dict[str, dict]):
    ret = {}

    for key, inner_dict in dictionaries.items():
        for inner_key, value in inner_dict.items():
            ret[inner_key] = ret.get(inner_key, {})
            ret[inner_key][key] = value
    return ret



def flatten_arrays(array_of_arrays):
    ret = []
    for array in array_of_arrays:
        ret.extend(array)
    return ret


def get_column(array_of_arrays, column):
    ret = []
    for array in array_of_arrays:
        ret.append(array[column])
    return ret


def iterate_dict_items_based_on_list_ordering(dct: dict, keys_ordering: list):
    for key in keys_ordering:
        assert key in keys_ordering
        yield key, dct[key]

def get_column_from_dict_of_list(dct_of_list: dict[Any, list[Any]], column_index: int, sorter=lambda x: x):
    two_d_list = list(dct_of_list.items())
    two_d_list = sorter(two_d_list)
    ret = []
    for key, value in two_d_list:
        ret.append(value[column_index])
    return ret



def groupby_and_select(lst: list, key_to_group_by, select=lambda x: x):
    grouping = {}
    for item in lst:
        key = key_to_group_by(item)
        if key not in grouping:
            grouping[key] = []
        grouping[key].append(select(item))
    return grouping




class Nothing:
    pass