import yaml
import logging
import logging.config

def setup_logger(name):
    with open('config/log_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
        return logging.getLogger(__name__)
