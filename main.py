import asyncio
import os.path
import re
from glob import glob

import namd_extractor
import pytorch_extractor
from glmark2_extractor import Glmark2ResultProcessor, MultiresolutionGlmark2ResultProcessor
from spreadsheet import SpreadsheetLogic
from stats_recap import StatRecapPerBenchmarkApp, StatRecapPerOpenStackService

glmark2_resolutions = ['1920x1080', '1366x768', '360x800', '192x108']
openstack_namd_batch_range = range(0, 15)


async def main():
    file_names = get_file_list()
    files = get_file_content_dict(file_names)

    glmark2_processors = {opnstck_svc_nm: MultiresolutionGlmark2ResultProcessor().add_resolutions(glmark2_resolutions)
                          for opnstck_svc_nm in files.keys()}
    pytorch_processors = {opnstck_svc_nm: pytorch_extractor.PytorchResultProcessor() for opnstck_svc_nm in files.keys()}
    namd_processors = {opnstck_svc_nm: namd_extractor.NamdResultProcessor() for opnstck_svc_nm in files.keys()}

    for index, (openstack_service_name, content) in enumerate(files.items()):
        print(openstack_service_name)
        benchmark_results = extract_file_name_from_more_format(content)
        for benchmark_type, content in benchmark_results.items():
            handle_processing(benchmark_type, content, pytorch_processors[openstack_service_name],
                              namd_processors[openstack_service_name], glmark2_processors[openstack_service_name])

    openstack_services = {opnstck_svc_nm: StatRecapPerOpenStackService(opnstck_svc_nm) for opnstck_svc_nm in files.keys()}
    for openstack_service_name, openstack_service_recap in openstack_services.items():
        openstack_service_recap.glmark2_processor = glmark2_processors[openstack_service_name]
        openstack_service_recap.pytorch_processor = pytorch_processors[openstack_service_name]
        openstack_service_recap.namd_processor = namd_processors[openstack_service_name]


    spreadsheet_logic = SpreadsheetLogic(openstack_services, glmark2_processors, namd_processors, pytorch_processors)
    print(spreadsheet_logic.url)

    await spreadsheet_logic.process_spreadsheet()

    with open("latex_command.tex", "w") as f:
        print("\n".join(StatRecapPerOpenStackService.as_latex_variables(
            list(openstack_services.values())
        )), file=f)



def handle_processing(benchmark_type, content, pytorch_processor, namd_processor, glmark2_processors: dict[str, Glmark2ResultProcessor]):
    handlers = {
        'pytorch_benchmark_result.txt': pytorch_processor,
        'namd_benchmark_result.txt': namd_processor,
    } | {
        f'glmark2_benchmark_result_{resolution}.txt': processor for resolution, processor in glmark2_processors.items()
    } | {
        f'namd_benchmark_result_{batch_no}.txt': namd_processor
        for batch_no in openstack_namd_batch_range
    }
    nop_processor = NopProcessor()
    handler = handlers.get(benchmark_type, nop_processor)
    handler.process(content)


def get_file_list() -> list[str]:
    return  list(glob("./data/*"))


def get_file_content_dict(file_list: list[str], file_name_extract=os.path.basename):
    ret = {}
    for file_path in file_list:
        key = file_name_extract(file_path)
        with open(file_path, "r") as f:
            ret[key] = f.read()
    return ret


"""
Format command linux `more *` di linux akan menghasilkan format:
::::::::::::::
FILENAME1
::::::::::::::
FILE_CONTENT1::::::::::::::
FILENAME2
::::::::::::::
FILE_CONTENT2::::::::::::::
...

fungsi ini memecah format itu 
"""
def extract_file_name_from_more_format(output_in_more_format: str) -> dict[str, str]:
    splitted = re.split(r':::::::*', output_in_more_format)
    splitted.pop(0)
    ret = {}
    for file_name, content in flat_to_2d(splitted, 2):
        ret[file_name.strip()] = content
    return ret



def flat_to_2d(lst: list, n: int):
    if len(lst) % n != 0:
        raise ValueError("Given list length is not divisible by n")
    for i in range(0, len(lst), n):
        yield lst[i:i+n]
    return


class NopProcessor:
    def process(self, content):
        return


if __name__ == "__main__":
    asyncio.run(main())


