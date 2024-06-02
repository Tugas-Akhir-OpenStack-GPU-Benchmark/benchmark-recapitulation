import re

import pandas as pd

from ResultProcessors import ResultProcessors
from stats import GREATER_THAN_PHYSICAL, DEFAULT_STATS_TO_CONSIDER


class NamdResultProcessor(ResultProcessors):
    def __init__(self):
        self.results = []

    def process(self, content) -> None:
        temp = list(re.finditer(r"ATPase Simulation - (\d|,)+ Atoms:", content))[0]
        content2 = content[temp.end(0):]
        extracted_content = re.split(r"Average:", content2)[0]
        benchmark_results_as_str = extracted_content.strip().split()
        self.results += list(map(float, benchmark_results_as_str))

    def groups_to_values_mapping(self) -> dict[str, list[float]]:
        return {'': self.results}

    def stats_to_consider(self) -> list[tuple[str, callable]]:
        return DEFAULT_STATS_TO_CONSIDER + GREATER_THAN_PHYSICAL

    def as_dataframe(self) -> pd.DataFrame:
        data = []
        for value in self.results:
            data.append({
                'days/ns': value,
            })
        return pd.DataFrame(data)
