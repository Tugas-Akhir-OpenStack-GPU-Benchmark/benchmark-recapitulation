import time

import gspread
from gspread import Worksheet

import glmark2_extractor
import namd_extractor
import pytorch_extractor
from glmark2_extractor import Glmark2ResultProcessor, MultiresolutionGlmark2ResultProcessor
from stats import stat_functions, T_test_equal, T_test_greater, major_grouping_by_stat_name, T_test_less
from stats_recap import StatRecapPerOpenStackService
from thread_pool_worker import WorkerPool, maximum_backoff
from utils import transpose, combine_dicts, flatten_dict_of_list, flatten_arrays, get_column, \
    iterate_dict_items_based_on_list_ordering, groupby_and_select


INDEX_OF_T_TEST_COMPARISON = 0


class SpreadsheetLogic:
    # openstack_service_name, then resolution
    def __init__(self, openstack_services_recap: dict[str, StatRecapPerOpenStackService],
                 glmark_processors: dict[str, MultiresolutionGlmark2ResultProcessor],
                 namd_processors: dict[str, namd_extractor.NamdResultProcessor],
                 pytorch_processors: dict[str, pytorch_extractor.PytorchResultProcessor], clear_sheet=True):
        self.gc = gspread.service_account("./key/key.json")
        self.url = "https://docs.google.com/spreadsheets/d/1tEoTYKBOAVweJ5HYzxigeq6tE--nBIjnK5f8EBO-JLk/edit#gid=0"
        self.document = self.gc.open_by_url(self.url)

        self.openstack_services_recap = openstack_services_recap
        self.glmark_processors = glmark_processors
        self.namd_processors = namd_processors
        self.pytorch_processors = pytorch_processors
        self.worksheets: list[gspread.Worksheet] = self.document.worksheets()
        self.workers = WorkerPool()
        self.spreadsheet_prefix = "08 "
        self.clear_sheet = clear_sheet

    def overview(self, openstack_services, glmark2_grouped_by_resolution: dict[str, list[Glmark2ResultProcessor]],
                 pytorch_grouped_by_model_batchsize_tc: dict[tuple[str, int, int], list]):

        headers = ["Benchmark",	"Group", "Stats"] + openstack_services

        comparison_openstack_service_name = openstack_services[INDEX_OF_T_TEST_COMPARISON]
        comparison = self.openstack_services_recap[comparison_openstack_service_name]
        for index, op_svc in enumerate(openstack_services):
            openstack_service_recap = self.openstack_services_recap[op_svc]
            openstack_service_recap.calculate_benchmark(comparison)

        table = StatRecapPerOpenStackService.as_table(get_column(
                list(iterate_dict_items_based_on_list_ordering(self.openstack_services_recap, openstack_services)),
            1
        ))
        table.insert(0, headers)

        self.draw_overview_table("Overview", table)
        major_grouping_by_stat_name(table)
        self.draw_overview_table("Overview by Stats", table)

    def draw_overview_table(self, sheetName, table):
        rekap_ws = self.get_or_create_worksheets(self.spreadsheet_prefix + sheetName)
        if self.clear_sheet:
            rekap_ws.clear()
        self.unmerge_sheet(rekap_ws)
        handle_write_req_limit(rekap_ws.update)(table, "A1")
        self.merge_adjacent_equal_rows(rekap_ws, get_column(table, 0), 'A', 1)
        self.merge_adjacent_equal_rows(rekap_ws, get_column(table, 1), 'B', 1)




    async def process_spreadsheet(self,):
        self.workers.start_workers(2)

        openstack_service_ordering = list(self.namd_processors.keys())
        glmark2_grouped_by_resolution = await self.process_glmark2(openstack_service_ordering)
        self.process_namd(openstack_service_ordering)
        pytorch_grouped_by_model_batchsize_tc = await self.process_pytorch(openstack_service_ordering)
        self.overview(openstack_service_ordering, glmark2_grouped_by_resolution, pytorch_grouped_by_model_batchsize_tc)

        self.workers.stop_workers()
        print("Done")

    async def process_glmark2(self, openstack_service_ordering):
        openstack_service_names = openstack_service_ordering
        dictionaries = []
        for ops_svc_name in openstack_service_ordering:
            dictionaries.append(self.glmark_processors[ops_svc_name].as_dict)
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
            handle_write_req_limit(glmark2_ws.clear)()
        self.unmerge_sheet(glmark2_ws)
        handle_write_req_limit(glmark2_ws.update)(table, "A1")
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
            handle_write_req_limit(namd_ws.clear)()
        handle_write_req_limit(namd_ws.update)([headers] + body_opstck_svc_as_col, "A1")

    async def process_pytorch(self, openstack_service_ordering):
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
            handle_write_req_limit(pytorch_ws.clear)()
        self.unmerge_sheet(pytorch_ws)
        handle_write_req_limit(pytorch_ws.update)(table, "A1")
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

    def unmerge_sheet(self, worksheet):
        requests = [
            {
                "unmergeCells": {
                    "range": {"sheetId": worksheet.id}
                }
            }
        ]
        handle_write_req_limit(self.document.batch_update)({"requests": requests})

def handle_write_req_limit(func):
    sleep_time = 1
    def wrapper(*args):
        nonlocal  sleep_time
        while True:
            try:
                return func(*args)
            except gspread.exceptions.APIError as e:
                if 'Quota exceeded for quota metric' not in e.response.text:
                    raise e
                time.sleep(sleep_time)
                sleep_time = min(sleep_time*2 , maximum_backoff)
    return wrapper



def func_to_str(func):
    return func.get_name()


