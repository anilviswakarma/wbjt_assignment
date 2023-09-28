from abc import ABC, abstractmethod

class AbstractStore():
    def __init__(self, config, **kwargs) -> None:
        self.config = config

    @abstractmethod
    def append(self, data: list, **kwargs) -> None:
        ...

    @abstractmethod
    def get_data(self):
        ...

    def __enrich_data__(self, item, provider_name, run_ts):
        item['provider'] = provider_name
        item['run_ts'] = run_ts
        return item