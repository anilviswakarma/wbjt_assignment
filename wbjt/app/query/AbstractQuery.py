from abc import ABC, abstractmethod

class AbstractQuery():
    def __init__(self, data_store) -> None:
        self.data_store = data_store
        
    @abstractmethod
    def get_cheapest_provider() -> str:
        ...

    @abstractmethod
    def get_cheapest_movie() -> str:
        ...
        


