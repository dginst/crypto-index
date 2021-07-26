import logging

from cryptoindex.index_wrap_func import daily_complete

# logging configuration
logging.basicConfig(filename='log_file.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logging.info('index_daily_launcher.py start')

daily_complete()

logging.info('index_daily_launcher.py end')
