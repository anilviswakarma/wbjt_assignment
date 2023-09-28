import pandas as pd
from pathlib import Path
from store.ParquetStore import ParquetStore
from query.AbstractQuery import AbstractQuery

class PandasQuery(AbstractQuery):
    """Class that uses pandas to query the data"""
    def __init__(self, data_store) -> None:
        super().__init__(data_store)
        if (not isinstance(ParquetStore)):
            raise Exception("PandasQuery engine works only with ParquetStore")
        
        self.path = data_store.get_store_path()
        # Workaround, Issue with the way parquet is being generated, does not read all the child folders
        fw_df = pd.read_parquet(Path.joinpath(self.path, 'provider=filmworld/')) 
        fw_df['provider'] = 'filmworld'   

        # Workaround, Issue with the way parquet is being generated, does not read all the child folders
        cw_df = pd.read_parquet(Path.joinpath(self.path, 'provider=cinemaworld/')) 
        cw_df['provider'] = 'cinemaworld'  

        self.all_data = pd.concat([fw_df, cw_df], ignore_index=True)

    def get_cheapest_movie(self) -> str:
        titles_df = self.all_data[['Title', 'Price']]
        cheapest_title_df = titles_df.groupby('Title').sum().reset_index()
        cheapest_title_name = cheapest_title_df[cheapest_title_df.Price == cheapest_title_df.Price.min()].Title.array[0]
        return cheapest_title_name

    def get_cheapest_provider(self) -> str:   
        self.all_data['Price'] = self.all_data['Price'].astype('Float64')
        provider_df = self.all_data[['provider', 'Price']]
        cheapest_provider = provider_df.groupby('provider').sum().reset_index()
        cheapest_provider_name = cheapest_provider[cheapest_provider.Price == cheapest_provider.Price.min()].provider.array[0]
        return cheapest_provider_name
