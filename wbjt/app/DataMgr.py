import shutil
from collectors.MovieDataCollector import MovieDataCollector
from pathlib import Path
from datetime import datetime
import logging
import pandas as pd
import numpy as np
import concurrent.futures

logger = logging.getLogger('root')

class DataMgr():

    def __init__(self, config):
        self.config = config
        self.concurrency = int(config['concurrency'])
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency)
        self.delete_history = bool(config['delete_history'])
        self.number_of_times_to_poll = int(config['number_of_times_to_poll'])

    def get_db_path(self):
        return Path.joinpath(Path.cwd(), self.config['db_path'])

    def delete_historical_data(self):
        """Deletes historical data if configured"""
        if self.delete_history:
            path = self.get_db_path()
            
            try:
                # removing directory
                shutil.rmtree(path)
            except FileNotFoundError:
                logger.info(f'Directory not found = {path}')

    def poll_and_write_data(self, apikeyname, apikeyvalue):
        """Polls for data from a movie provider and writes it to destination
        Data is polled n times for each provider based on the 'number_of_times_to_poll' config"""
        
        for n in range(0, self.number_of_times_to_poll):
            logger.info(f'Intiated polling attempt = {n}')
            
            # Add apikey to provider configs and fetch data and write
            for provider_conf in self.config['movie_data_providers']:
                provider_conf['access_token'] = {apikeyname: apikeyvalue}
                self.__poll_data_and_write__(provider_conf)

    def __poll_data_and_write__(self, provider_conf):
        """Polls data for a provider and write to destination in parquet format"""

        # Get run_ts upto the second as we dont want partitions to overlap for different runs
        run_ts = datetime.now().strftime('%Y%m%d%H%M%S')
        
        data = [movie for movie in MovieDataCollector(provider_conf, self.thread_pool).get_movies_data() if movie is not None]

        if len(data) > 0:
            logger.info(f'Data found, provider ={provider_conf["name"]}, run_ts = {run_ts}, count = {len(data)} ')

            df = pd.DataFrame(data)

            # Add awards col as Filmworld does not provide it
            if provider_conf['name'] == 'filmworld':
                df['Awards'] = np.nan 
            
            self.writeAsParquet(provider_conf, run_ts, df)
        else:
                logger.info(f'No data found, provider ={provider_conf["name"]}, run_ts = {run_ts}')

    def writeAsParquet(self, provider_conf, run_ts, df):
        """Write data in parquet format, partitioned by provider and run_ts"""
        
        logger.info(f'Provider = {provider_conf["name"]}, Shape =  {df.shape}')
        df['provider'] = provider_conf["name"]
        df['run_ts'] = run_ts


        logger.info(f"Writing data to path {self.get_db_path()}")
        df.to_parquet(self.get_db_path(), partition_cols=['provider', 'run_ts'])



    def readParquetData(self) -> pd.DataFrame:
        """Reads the whole movies data and returns as a Dataframe"""
        return pd.read_parquet(self.get_data_path())









