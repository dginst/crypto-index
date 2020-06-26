from crontab import CronTab

ecb_daily_download = 'python3 /home/ubuntu/crypto-index/ecb_daily_download.py & '
ecb_daily_setup = 'python3 /home/ubuntu/crypto-index/ecb_daily_setup.py & '
cw_daily_download = 'python3 /home/ubuntu/crypto-index/cw_daily_download.py & '
cw_daily_cleaning = 'python3 /home/ubuntu/crypto-index/cw_daily_cleaning.py & '
cw_daily_conversion = 'python3 /home/ubuntu/crypto-index/cw_daily_conversion.py & '
exc_daily_cleaning = 'python3 /home/ubuntu/crypto-index/exc_daily_cleaning.py & '
exc_daily_conversion = 'python3 /home/ubuntu/crypto-index/exc_daily_conversion.py & '
index_daily = 'python3 /home/ubuntu/crypto-index/index_daily.py & '
index_board = 'python3 /home/ubuntu/crypto-index/index_board.py & '
index_start_quarter = 'python3 /home/ubuntu/crypto-index/index_start_quarter.py & '
index_data_feed = 'python3 /home/ubuntu/crypto-index/index_data_feed.py & '
