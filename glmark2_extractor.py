import re


class Glmark2ResultProcessor:
    def __init__(self):
        self.results = {}

    def process(self, content) -> None:
        informations = list(re.finditer(
            "\\[([a-zA-Z]+)\\] (.+?): FPS: (\\d+) FrameTime: ([0-9.]+) ms", content))
        for info in informations:
            step_category = info.group(1)
            step_name = info.group(2)
            fps = info.group(3)
            frame_time = info.group(4)
            self.results[step_name] = (step_category, int(fps), float(frame_time))

    def get_values(self):
        select_by_fps_result = lambda x: x[1]
        list_of_tuple_of_category_fps_spf = list(self.results.values())
        return list(map(select_by_fps_result, list_of_tuple_of_category_fps_spf))
