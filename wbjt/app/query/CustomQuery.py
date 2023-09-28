from store.MemoryStore import MemoryStore
from query.AbstractQuery import AbstractQuery

class CustomQuery(AbstractQuery):
    """Implements custom written queries"""
    def __init__(self, data_store) -> None:
        super().__init__(data_store)
        if (not isinstance(data_store, MemoryStore)):
            found_typ = type(data_store)
            raise Exception(f'CustomQuery engine works only with MemmoryStore, found {found_typ}')
        
        self.data = data_store.get_data()

    def __get_sorted_overall_prices_over_column__(self, data: list, col_name:str):
        overall_prices = dict()

        # We are aggregating prices over the column passed
        for movie in data:
            key = movie[col_name]
            price = float(movie['Price'])

            if overall_prices.get(key):
                overall_prices[key] = overall_prices[key] + price
            else:
                overall_prices[key] = price

        return sorted(list(overall_prices.items()), key= lambda movie: movie[1])

    def get_cheapest_movie(self) -> str:
        sorted_titles = self.__get_sorted_overall_prices_over_column__(self.data, 'Title')
        return sorted_titles[0][0]
        

    def get_cheapest_provider(self) -> str:
        sorted_providers = self.__get_sorted_overall_prices_over_column__(self.data, 'provider')
        return sorted_providers[0][0]

