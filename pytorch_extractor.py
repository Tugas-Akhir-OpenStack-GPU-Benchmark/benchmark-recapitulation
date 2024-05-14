import re

from ResultProcessors import ResultProcessors


class PytorchResultProcessor(ResultProcessors):
    def __init__(self):
        self.results = {}

    def process(self, content) -> None:
        separator_beginning = list(re.finditer(r"Device: ([a-zA-Z0-9]+) - Batch Size: (\d+) - Model: ([a-zA-Z0-9\-_]+):", content))
        separator_ending = list(re.finditer(r"Average: ([.0-9]+) batches/sec", content))

        beginning: re.Match
        ending: re.Match
        for beginning, ending in zip(separator_beginning, separator_ending):
            batch_size = int(beginning.group(2))
            model = beginning.group(3)

            substring = content[beginning.end(0):ending.start(0)]
            result = substring.split()
            self.results[(model, batch_size)] = list(map(float, result))

    def groups_to_values_mapping(self) -> dict[str, list[float]]:
        ret = {}
        for (model, batch_size), values in self.results.items():
            if model not in ret:
                ret[model] = []
            ret[model].extend(values)
        return ret
