class ResultProcessors:
    def process(self, content) -> None:
        raise NotImplementedError

    def groups_to_values_mapping(self) -> dict[str, list[float]]:
        raise NotImplementedError

    def stats_to_consider(self) -> list[tuple[str, callable]]:
        raise NotImplementedError
