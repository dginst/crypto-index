from logging import basicConfig, info, INFO

from cryptoindex.index_wrap_func import daily_complete

# logging configuration
basicConfig(filename='log_file.log', filemode='a',
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=INFO)

info('index_daily_launcher.py start')

daily_complete()

info('index_daily_launcher.py end')
