from __future__ import annotations
import re

import pandas as pd

from ResultProcessors import ResultProcessors
from stats import DEFAULT_STATS_TO_CONSIDER, LESS_THAN_PHYSICAL


class MultiresolutionGlmark2ResultProcessor(ResultProcessors):
    def __init__(self):
        self.resolution_to_processor_mapping = {}

    @property
    def as_dict(self):
        return self.resolution_to_processor_mapping

    def items(self):
        return self.resolution_to_processor_mapping.items()

    def groups_to_values_mapping(self) -> dict[str, list[float]]:
        ret = {}
        glmark2Processor: Glmark2ResultProcessor
        for resolution, glmark2Processor in self.resolution_to_processor_mapping.items():
            ret[resolution] = glmark2Processor.get_values()
        return ret

    @property
    def resolutions(self):
        return self.resolution_to_processor_mapping.keys()

    def add_resolutions(self, resolutions):
        for resolution in resolutions:
            if resolution not in self.resolution_to_processor_mapping:
                self.resolution_to_processor_mapping[resolution] = Glmark2ResultProcessor()
        return self

    def process(self, resolution, content):
        if resolution not in self.resolution_to_processor_mapping:
            self.resolution_to_processor_mapping[resolution] = Glmark2ResultProcessor()
        self.resolution_to_processor_mapping[resolution].process(content)

    def stats_to_consider(self):
        return DEFAULT_STATS_TO_CONSIDER + LESS_THAN_PHYSICAL

    def as_dataframe(self) -> pd.DataFrame:
        data = []
        for resolution, values in self.groups_to_values_mapping().items():
            for value in values:
                data.append({
                    'resolution': resolution,
                    'FPS': value,
                })
        return pd.DataFrame(data)



class Glmark2ResultProcessor:
    def __init__(self):
        self.results = {}

    def process(self, content) -> None:
        informations = list(re.finditer(
            "\\[([a-zA-Z0-9]+)\\] (.+?): FPS: (\\d+) FrameTime: ([0-9.]+) ms", content))
        for info in informations:
            step_category = info.group(1)
            step_name = info.group(2)
            fps = info.group(3)
            frame_time = info.group(4)
            self.results[f"{step_category}-{step_name}"] = (step_category, int(fps), float(frame_time))

    def get_values(self):
        select_by_fps_result = lambda x: x[1]
        list_of_tuple_of_category_fps_spf = list(self.results.values())
        return list(map(select_by_fps_result, list_of_tuple_of_category_fps_spf))
