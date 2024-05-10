import gspread
from gspread import Worksheet

import glmark2_extractor
import namd_extractor
import pytorch_extractor
from glmark2_extractor import Glmark2ResultProcessor
from stats import stat_functions
from thread_pool_worker import WorkerPool
from utils import transpose, combine_dicts, flatten_dict_of_list, flatten_arrays, get_column, \
    iterate_dict_items_based_on_list_ordering, groupby_and_select


class SpreadsheetLogic:
    # openstack_service_name, then resolution
    def __init__(self, glmark_processors: dict[str, dict[str, glmark2_extractor.Glmark2ResultProcessor]],
                 namd_processors: dict[str, namd_extractor.NamdResultProcessor],
                 pytorch_processors: dict[str, pytorch_extractor.PytorchResultProcessor], clear_sheet=False):
        self.gc = gspread.service_account("./key/key.json")
        self.document = self.gc.open_by_url("https://docs.google.com/spreadsheets/d/1tEoTYKBOAVweJ5HYzxigeq6tE--nBIjnK5f8EBO-JLk/edit#gid=0")
        self.glmark_processors = glmark_processors
        self.namd_processors = namd_processors
        self.pytorch_processors = pytorch_processors
        self.worksheets: list[gspread.Worksheet] = self.document.worksheets()
        self.workers = WorkerPool()
        self.spreadsheet_prefix = "07 "
        self.clear_sheet = clear_sheet

    def overview(self, glmark2_grouped_by_resolution: dict[str, list[Glmark2ResultProcessor]],
                 pytorch_grouped_by_model_batchsize_tc: dict[tuple[str, int, int], list]):
        openstack_services = list(self.glmark_processors.keys())
        headers = ["Benchmark",	"Group", "Stats"] + openstack_services
        table = [headers]
        for resolution, benchmark_results in glmark2_grouped_by_resolution.items():
            for stat_function in stat_functions:
                row = ['Glmark2', resolution, stat_function.__name__]
                for benchmark_result in benchmark_results:
                    select_by_fps_result = lambda x: x[1]
                    list_of_tuple_of_category_fps_spf = list(benchmark_result.results.values())
                    list_of_fps = list(map(select_by_fps_result, list_of_tuple_of_category_fps_spf))
                    row.append(stat_function(list_of_fps))
                table.append(row)
        for stat_function in stat_functions:
            row = ['NAMD', '', stat_function.__name__]
            for openstack_service in openstack_services:
                benchmark_results_namd = self.namd_processors[openstack_service]
                row.append(stat_function(benchmark_results_namd.results))
            table.append(row)
        # list of ((model, batch_size, tc_number), values)
        pytorch_list = list(pytorch_grouped_by_model_batchsize_tc.items())
        group_by_model = lambda x: x[0][0]
        select_values = lambda x: x[1]
        model_to_list_of_values = groupby_and_select(pytorch_list, group_by_model, select_values)
        for model, list_of_values in model_to_list_of_values.items():
            # list_of_values contains 2d list, 1st axis is batch_size & tc. 2nd index is the openstack_service

            transposed_list_of_values = transpose(list_of_values)
            for stat_function in stat_functions:
                table.append(['PyTorch', model, stat_function.__name__, ] + [
                    stat_function(values) for values in transposed_list_of_values
                ])
        rekap_ws = self.get_or_create_worksheets(self.spreadsheet_prefix + "Overview")
        if self.clear_sheet:
            rekap_ws.clear()
        rekap_ws.update(table, "A1")
        self.merge_adjacent_equal_rows(rekap_ws, get_column(table, 0), 'A', 1)
        self.merge_adjacent_equal_rows(rekap_ws, get_column(table, 1), 'B', 1)



    def process_spreadsheet(self,):
        self.workers.start_workers(2)

        openstack_service_ordering = list(self.namd_processors.keys())
        glmark2_grouped_by_resolution = self.process_glmark2(openstack_service_ordering)
        self.process_namd(openstack_service_ordering)
        pytorch_grouped_by_model_batchsize_tc = self.process_pytorch(openstack_service_ordering)
        self.overview(glmark2_grouped_by_resolution, pytorch_grouped_by_model_batchsize_tc)

        self.workers.stop_workers()

    def process_glmark2(self, openstack_service_ordering):
        openstack_service_names = openstack_service_ordering
        dictionaries = []
        for ops_svc_name in openstack_service_ordering:
            dictionaries.append(self.glmark_processors[ops_svc_name])
        ret = {}
        headers = ['resolution', 'step']
        table = [headers]
        # key: resolution, value: list of performance with the same ordering as openstack_service_names
        sorter = lambda x: sorted(x, key=lambda y: (len(y), y))
        grouped_by_resolution = combine_dicts(dictionaries, sorter, jagged_default_value=None)
        for service_name in openstack_service_names:
            headers.append(f"{service_name} (FPS)")

        for resolution, benchmark_results in grouped_by_resolution.items():
            benchmark_result: Glmark2ResultProcessor
            for index, benchmark_result in enumerate(benchmark_results):
                service_name = openstack_service_names[index]
                for step_name, performance in benchmark_result.results.items():
                    key = (resolution, step_name)
                    if key not in ret:
                        ret[key] = []
                    ret[key].append(performance[1])
        for key, values in ret.items():
            table.append([*key, *values])

        glmark2_ws = self.get_or_create_worksheets(self.spreadsheet_prefix + "Glmark2")
        if self.clear_sheet:
            glmark2_ws.clear()
        glmark2_ws.update(table, "A1")
        self.merge_adjacent_equal_rows(glmark2_ws, get_column(table, 0), 'A', 1)
        return grouped_by_resolution


    def process_namd(self, openstack_service_ordering):
        headers = []
        for openstack_service in openstack_service_ordering:
            headers.append(f"{openstack_service} (days/ns)")
        body_opstck_svc_as_row = []
        for openstack_service, benchmark in iterate_dict_items_based_on_list_ordering(self.namd_processors, openstack_service_ordering):
            body_opstck_svc_as_row.append(benchmark.results)
        body_opstck_svc_as_col = transpose(body_opstck_svc_as_row)

        namd_ws = self.get_or_create_worksheets(self.spreadsheet_prefix + "NAMD")
        if self.clear_sheet:
            namd_ws.clear()
        namd_ws.update([headers] + body_opstck_svc_as_col, "A1")

    def process_pytorch(self, openstack_service_ordering):
        dictionaries = []
        for openstack_service, benchmark in iterate_dict_items_based_on_list_ordering(self.pytorch_processors, openstack_service_ordering):
            flatten_benchmark_results = flatten_dict_of_list(benchmark.results)
            dictionaries.append(flatten_benchmark_results)
        combined_dict = combine_dicts(dictionaries, sorted, jagged_default_value=None)
        headers = ["Model", "Batch Size", "Test Case"]
        table = [headers]
        for openstack_service in self.pytorch_processors.keys():
            headers.append(f"{openstack_service} (batches/s)")
        for (model, batch_size, tc_number), values in combined_dict.items():
            table.append([model, int(batch_size), tc_number, *values])

        pytorch_ws = self.get_or_create_worksheets(self.spreadsheet_prefix + "PyTorch")
        if self.clear_sheet:
            pytorch_ws.clear()
        pytorch_ws.update(table, "A1")
        self.merge_adjacent_equal_rows(pytorch_ws, get_column(table, 0), 'A', 1)
        self.merge_adjacent_equal_rows(pytorch_ws, get_column(table, 1), 'B', 1)
        return combined_dict


    def get_or_create_worksheets(self, name):
        for ws in self.worksheets:
            if ws.title == name:
                return ws
        worksheet = self.document.add_worksheet(name, 0, 0)
        self.worksheets.append(name)
        return worksheet

    def merge_adjacent_equal_rows(self, worksheet: Worksheet, row_values: list, column_spreadsheet_index='A', row_spreadsheet_start_index=1):
        if len(row_values) == 0:
            return
        prev_value = row_values[0]
        start = 0
        end = 0

        def get_task(start, end):
            return lambda: worksheet.merge_cells(
                f"{column_spreadsheet_index}{start+row_spreadsheet_start_index}:{column_spreadsheet_index}{end+row_spreadsheet_start_index}")

        for i, value in enumerate(row_values):
            if value != prev_value:
                self.workers.add_task(get_task(start, end))
                start = i
                prev_value = value
            end = i
        self.workers.add_task(get_task(start, end))






