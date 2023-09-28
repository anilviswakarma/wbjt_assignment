import logging
import shutil
import pandas as pd

from store.AbstractStore import AbstractStore
from pathlib import Path


logger = logging.getLogger('root')

class ParquetStore(AbstractStore):
    """Stores data in parquet format"""
    def __init__(self, config, **kwargs) -> None:
        super().__init__(config)
        self.path = config['store']['path']
        
    def get_store_path(self):
        return Path.joinpath(Path.cwd(), self.path)
    
    def append(self, new_data, provider_name, run_ts):
        """Writes data in parquet format, partitioned by provider and run_ts"""
        
        df = pd.DataFrame(new_data)
        df['provider'] = provider_name
        df['run_ts'] = run_ts

        logger.info(f"Writing data to path {self.get_store_path()}")
        df.to_parquet(self.get_store_path(), partition_cols=['provider', 'run_ts'])

    def get_data(self) -> list:
        return super().get_data()
    

    def delete_data(self):
        """Deletes data if configured"""
        if self.delete_history:
            path = self.get_db_path()
            try:
                # removing directory
                shutil.rmtree(path)
            except FileNotFoundError:
                logger.info(f'Directory not found = {path}')

        

