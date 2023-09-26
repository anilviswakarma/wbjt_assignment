import argparse
import yaml
import log
from pathlib import Path
import pandas as pd


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

    # Delete historical data if configured
    if (config['delete_history']):
        dm.delete_historical_data()

    if (not args.apikey):
        raise Exception('Api key required for polling')

    apikeyname, apikeyvalue = args.apikey.split('=')[0], args.apikey.split('=')[1]
    dm.poll_and_write_data(apikeyname, apikeyvalue)


    db_path = dm.get_db_path()

    # Analyze
    logger.info("Analyzing the data.....")

    # Workaround, Issue with the way parquet is being generated, does not read all the child folders
    fw_df = pd.read_parquet(Path.joinpath(db_path, 'provider=filmworld/')) 
    fw_df['provider'] = 'filmworld' 


    # Workaround, Issue with the way parquet is being generated, does not read all the child folders
    cw_df = pd.read_parquet(Path.joinpath(db_path, 'provider=cinemaworld/')) 
    cw_df['provider'] = 'cinemaworld'

    all_data = pd.concat([fw_df, cw_df], ignore_index=True)

    all_data['Price'] = all_data['Price'].astype('Float64')
    provider_df = all_data[['provider', 'Price']]
    cheapest_provider = provider_df.groupby('provider').sum().reset_index()

    cheapest_provider_name = cheapest_provider[cheapest_provider.Price == cheapest_provider.Price.min()].provider.array[0]
    logger.info(f"Cheapest provider => {cheapest_provider_name}")


    titles_df = all_data[['Title', 'Price']]
    cheapest_title_df = titles_df.groupby('Title').sum().reset_index()
    cheapest_title_name = cheapest_title_df[cheapest_title_df.Price == cheapest_title_df.Price.min()].Title.array[0]
    logger.info(f"Cheapest title => {cheapest_title_name}")

