from crontab import CronTab

cron = CronTab(user='ubuntu')
job = cron.new(command='python3 /home/ubuntu/crypto-index/cron-test.py')

job.minute.on(15)
job.hour.on(15)

job2 = cron.new(command='python3 /home/ubuntu/crypto-index/cron-test1.py')

job2.minute.on(15)
job2.hour.on(15)

job3 = cron.new(command='python3 /home/ubuntu/crypto-index/ECB_daily.py')

job3.minute.on(0)
job3.hour.on(0)

cron.write()