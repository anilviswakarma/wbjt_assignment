from store.AbstractStore import AbstractStore
import logging

logger = logging.getLogger('root')

class MemoryStore(AbstractStore):
    def __init__(self, config, **kwargs) -> None:
        super().__init__(config)
        self.data = []
    
    def append(self, new_data: list, provider_name, run_ts) -> None:
        enriched_data = map(lambda d: self.__enrich_data__(d, provider_name, run_ts), new_data)
        self.data.extend(enriched_data)

    def get_data(self) -> list:
        return self.data
