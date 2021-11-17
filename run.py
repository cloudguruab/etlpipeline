"""
entry point into application
"""
import logging
import logging.config
import yaml
from os.path import join, dirname, realpath

def main():
    """
    manager for running etl job
    """
    #parse yaml file
    config_path = join(dirname(realpath(__file__)), 'config/conf.yml')
    config = yaml.safe_load(open(config_path))

    #configure loggging
    log_config = config['logging']
    logging.config.dictConfig(log_config) 
    logger = logging.getLogger(__name__)
    logger.info("this is a test")
    
        
if __name__ == '__main__':
    main()