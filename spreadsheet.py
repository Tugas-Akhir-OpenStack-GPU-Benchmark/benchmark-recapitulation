import gspread
from gspread import Worksheet

import glmark2_extractor
import namd_extractor
import pytorch_extractor
from glmark2_extractor import Glmark2ResultProcessor
from thread_pool_worker import WorkerPool
from utils import transpose, combine_dicts, flatten_dict_of_list, flatten_arrays, get_column


class SpreadsheetLogic:
    def __init__(self, glmark_processors: dict[str, dict[str, glmark2_extractor.Glmark2ResultProcessor]],
                 namd_processors: dict[str, namd_extractor.NamdResultProcessor],
                 pytorch_processors: dict[str, pytorch_extractor.PytorchResultProcessor]):
        self.gc = gspread.service_account("./key/key.json")
        self.document = self.gc.open_by_url("https://docs.google.com/spreadsheets/d/1tEoTYKBOAVweJ5HYzxigeq6tE--nBIjnK5f8EBO-JLk/edit#gid=0")
        self.glmark_processors = glmark_processors
        self.namd_processors = namd_processors
        self.pytorch_processors = pytorch_processors
        self.worksheets: list[gspread.Worksheet] = self.document.worksheets()
        self.workers = WorkerPool()

    def process_spreadsheet(self,):
        self.workers.start_workers(2)
        self.process_glmark2()
        self.process_namd()
        self.process_pytorch()

        self.workers.stop_workers()

    def process_glmark2(self):
        openstack_service_names = []
        dictionaries = []
        for ops_svc_name, dct in self.glmark_processors.items():
            openstack_service_names.append(ops_svc_name)
            dictionaries.append(dct)
        ret = {}
        headers = ['resolution', 'step']
        table = [headers]
        # key: resolution, value: list of performance with the same ordering as openstack_service_names
        sorter = lambda x: sorted(x, key=lambda y: (len(y), y))
        grouped_by_resolution = combine_dicts(dictionaries, sorter, jagged_default_value=None)
        headers.extend(openstack_service_names)

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

        glmark2_ws = self.get_or_create_worksheets("Glmark2")
        glmark2_ws.clear()
        glmark2_ws.update(table, "A1")
        self.merge_adjacent_equal_rows(glmark2_ws, get_column(table, 0), 'A', 1)


    def process_namd(self):
        headers = []
        for openstack_service in self.namd_processors.keys():
            headers.append(openstack_service)
        body_opstck_svc_as_row = []
        for openstack_service, benchmark in self.namd_processors.items():
            body_opstck_svc_as_row.append(benchmark.results)
        body_opstck_svc_as_col = transpose(body_opstck_svc_as_row)

        namd_ws = self.get_or_create_worksheets("NAMD")
        namd_ws.clear()
        namd_ws.update([headers] + body_opstck_svc_as_col, "A1")

    def process_pytorch(self):
        dictionaries = []
        for openstack_service, benchmark in self.pytorch_processors.items():
            flattern_benchmark_results = flatten_dict_of_list(benchmark.results)
            dictionaries.append(flattern_benchmark_results)
        combined_dict = combine_dicts(dictionaries, sorted, jagged_default_value=None)
        headers = ["Model", "Batch Size", "Test Case"]
        table = [headers]
        for openstack_service in self.pytorch_processors.keys():
            headers.append(openstack_service)
        for (model, batch_size, tc_number), values in combined_dict.items():
            table.append([model, int(batch_size), tc_number, *values])

        pytorch_ws = self.get_or_create_worksheets("PyTorch")
        pytorch_ws.clear()
        pytorch_ws.update(table, "A1")
        self.merge_adjacent_equal_rows(pytorch_ws, get_column(table, 0), 'A', 1)
        self.merge_adjacent_equal_rows(pytorch_ws, get_column(table, 1), 'B', 1)



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






