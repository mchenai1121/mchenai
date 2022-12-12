import schedule
import time
from functions import check_date

schedule.every().monday.at("18:00").do(check_date)
schedule.every().wednesday.at("18:00").do(check_date)

while True:
    schedule.run_pending()
    time.sleep(1)