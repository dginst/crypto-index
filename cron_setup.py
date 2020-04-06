from crontab import CronTab



bitflyer_path = 'python3 /home/ubuntu/crypto-index/caller-bitflyer.py & '
bitstamp_path = 'python3 /home/ubuntu/crypto-index/caller-bitstamp.py & '
bittrex_path = 'python3 /home/ubuntu/crypto-index/caller-bittrex.py & '
coinbase_path = 'python3 /home/ubuntu/crypto-index/caller-coinbase.py & '
gemini_path = 'python3 /home/ubuntu/crypto-index/caller-gemini.py & '
itbit_path = 'python3 /home/ubuntu/crypto-index/caller-itbit.py & '
kraken_path ='python3 /home/ubuntu/crypto-index/caller-kraken.py & '
poloniex_path = 'python3 /home/ubuntu/crypto-index/caller-poloniex.py'

cron = CronTab(user = 'ubuntu')
job = cron.new(command = bitflyer_path + bitstamp_path + bittrex_path + coinbase_path + gemini_path + itbit_path + kraken_path + poloniex_path )

job.minute.on(15)
job.hour.on(14)

job1 = job

job1.minute.on(15)
job1.hour.on(15)

job3 = job

job3.minute.on(0)
job3.hour.on(0)

cron.write()