import logging
from adapters.RequestAdapter import RequestAdapter
from urllib.parse import urljoin

import concurrent.futures

logger = logging.getLogger('root')

class MovieDataCollector():
    """Class to handle collection of movies data"""
    def __init__(self, config, thread_pool):
        self.thread_pool = thread_pool
        self.config =  config
        self.name = config['name']
        self.base_url = config['base_url']
        self.summary_api = config['summary_api']
        self.details_api = config['details_api']
        self.timeout = config['timeout']
        self.access_token = config['access_token']
        
        self.session = RequestAdapter(base_url=self.base_url,
                                        headers=self.access_token,
                                        timeout=self.timeout,
                                        accepted_format = 'application/json')
        
        logger.debug(f'Initialized data collector for {self.name} db.')
        

    def get_movies_data(self) -> dict:
        """This method gets movie data from a given data source"""
        summary_response_list = self.__get_movies_summary__()

        # Map to movie ids and get each movie details
        movie_ids = map(lambda item: item['ID'] if item.get('ID') else None, summary_response_list)
        return self.thread_pool.map(lambda id: self.__get_movie_details__(id), movie_ids)

    def __get_movies_summary__(self) -> list:
        """Private method to fetch the data from movies database"""
        
        resp = self.session.get(self.summary_api)

        # Read the summary response in json format if present else init empty dict
        response = resp.json() if resp else dict()
        summary_response_list = response['Movies'] if response.get('Movies') else []
        logger.debug(f'Received {len(summary_response_list)} movies from {self.name}')

        return summary_response_list
    
    def __get_movie_details__(self, movie_id: str) -> dict:
        """Private method to get movie details"""
        movie_details_url = urljoin(self.details_api, url = movie_id)
        logger.debug(f'Getting data for sub_url {movie_details_url}')
        r = self.session.get(movie_details_url)
        return r.json() if r else None










        







