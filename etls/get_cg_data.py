import requests, sys, os

import datetime, time
from collections import defaultdict
import pandas as pd
path = os.path.join(os.path.dirname(__file__), '../utils/')
sys.path.insert(0, path )

from constants import CG_API_KEY

def get_cg_data(coin_id, start_date, end_date):
    start_date_unix = time.mktime(start_date.timetuple())
    end_date_unix = time.mktime(end_date.timetuple())
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range?vs_currency=usd&from={start_date_unix}&to={end_date_unix}'
    r = requests.get(url, headers={'x-cg-demo-api-key': CG_API_KEY})
    data = r.json()

    data_store = defaultdict(list)

    for item in data['prices']:
        data_store['timestamp'].append(item[0])
        data_store['prices'].append(item[1])
    df = pd.DataFrame.from_dict(data_store)
    df['date'] = pd.to_datetime(df['timestamp'],unit='ms')
    return df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)

# start_date = datetime.date(2024,3,9)
# end_date = start_date + datetime.timedelta(days=1)
# data = get_cg_data('bitcoin',start_date, end_date)

# print(data)