import requests
from requests_toolbelt import sessions
import logging


logger = logging.getLogger('root')

class RequestAdapter():
    """A request session adapter, maintains the session object and ensures multiple sessions we dont open multiple sessions to a single url.
    Also has the added functionality of forcing user to specify timeout, as these session objects are designed to be used with Api 
    which may be flakey"""

    def __init__(self, base_url: str, headers: dict, timeout: int, accepted_format = 'application/json') -> None:
        """Initializes the object, required parameter are
        1. base_url: Base url for the session
        2. headers: any headers to be passed, can include the api-key among other things
        3. timeout: timeout for the request in seconds
        4. accepted_format: the format in which client assume to accept the data
        """
        self.base_url = base_url
        self.headers = headers
        self.timeout = timeout
        self.accepted_format = accepted_format

        logger.debug(f'Session created for Base_Url = {self.base_url}')
        self.session = sessions.BaseUrlSession(self.base_url)
    
    def get(self, sub_path, params = None):
        """Sends a get request to the base_url + sub_path + params"""
        if (len(sub_path) == 0):
            raise Exception(f'Bad sub_path {sub_path}')
        logger.debug(f'Sending request for sub_path {sub_path}, with params = {params}')
        headers = self.headers

        try: 
            return self.session.get(url=sub_path, headers=headers, params=params, timeout=self.timeout)
        except requests.exceptions.Timeout:
            logger.warning('The request timed out')
            return None


