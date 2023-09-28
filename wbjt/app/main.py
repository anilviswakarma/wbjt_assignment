import argparse
import yaml
from query.PandasQuery import PandasQuery
from query.CustomQuery import CustomQuery
import log
from pathlib import Path


# Setup logging
logger = log.setup_logger('root')

if __name__ == "__main__":
        
    # Parse required arguments
    parser = argparse.ArgumentParser(
                        prog='MoviesDb data collector and analyzer',
                        description='Pulls data from movies db and analyzes the data')

    parser.add_argument('--apikey', required=False, help='Must pass the api key, in the format name=value')

    args = parser.parse_args()

    # Load config
    config = {}
    with open('config/app_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())

    # Instantiate the datamgr
    from DataMgr import DataMgr
    dm = DataMgr(config)

    if (not args.apikey):
        raise Exception('Api key required for polling')

    apikeyname, apikeyvalue = args.apikey.split('=')[0], args.apikey.split('=')[1]
    dm.poll_and_write_data(apikeyname, apikeyvalue)

    # Analyze
    logger.info("Analyzing the data.....")

    qe = None
    if config['query_engine']['type'] == 'pandas':
        qe = PandasQuery(dm.get_data_store())
    elif config['query_engine']['type'] == 'custom':
        qe = CustomQuery(dm.get_data_store())
    else:
        raise Exception("Unknown QE defined")


    logger.info(f"Cheapest provider => {qe.get_cheapest_provider()}")
    logger.info(f"Cheapest title => {qe.get_cheapest_movie()}")

    logger.info('Done.')

