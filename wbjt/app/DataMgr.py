import shutil
from store.MemoryStore import MemoryStore
from store.ParquetStore import ParquetStore
from collectors.MovieDataCollector import MovieDataCollector
from datetime import datetime
import logging

import concurrent.futures

logger = logging.getLogger('root')

class DataMgr():

    def __init__(self, config):
        self.config = config
        self.concurrency = int(config['concurrency'])
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency)
        self.number_of_times_to_poll = int(config['number_of_times_to_poll'])
        storetype = config['store']['type']
        
        if storetype == 'parquet':
            self.store = ParquetStore(config)
        else:
            # Default to memory store
            self.store = MemoryStore(config)

    def get_data_store(self):
        return self.store

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
            self.store.append(data, provider_name=provider_conf['name'], run_ts=run_ts)
        else:
                logger.info(f'No data found, provider ={provider_conf["name"]}, run_ts = {run_ts}')









