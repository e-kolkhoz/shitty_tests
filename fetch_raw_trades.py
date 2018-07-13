import time
import ccxt
import pandas as pd

from datetime import datetime, timedelta

import threading

exchanges = [
    (ccxt.binance(), 'BTC/USDT'),
    #(ccxt.bitfinex(), 'BTC/USDT'),
    #(ccxt.gdax(), 'BTC/USD'),
  #  (ccxt.kraken(), 'BTC/USD'),
    #(ccxt.okex(), 'BTC/USDT'),
   # (ccxt.yobit(), 'BTC/USD'),
]

#RECENT_DAYS = 100
RECENT_DAYS = 7


END_DT = datetime.now().date()
END_DT_TS = int(END_DT.strftime('%s')) * 1000

START_DT = END_DT - timedelta(days=RECENT_DAYS)
START_DT_TS = int(START_DT.strftime('%s')) * 1000


def to_datetime(ts):
    return datetime.fromtimestamp(int(ts) // 1000)

#иначе мейкера от тейкера не отличить
def get_type(d, exname):
    if d['type'] in ['market', 'limit']:
        return d['type']
    elif "m" in d['info']:
        if d['info']['m'] == True:
            return 'limit'
        elif d['info']['m'] == False:
            return 'market'
        else:
            raise
    else:
        raise



def process_trades(dd, ex_name):
    trades = [{
        'type': get_type(d, ex_name),
        'timestamp': d['timestamp'],
        'vol': d['amount'],
        'tid': d['id'],
        'price':d['price'],
        'side':d['side']
    } for d in dd]
    ftime = trades[0]['timestamp']
    ltime = trades[-1]['timestamp']
    return ftime, ltime, trades


def fetch_ex(api, sym):

    api.load_markets()

    ex_name = api.id
    trades = api.fetch_trades(symbol=sym, since=START_DT_TS, limit=100000)
    ftime, ltime, trades = process_trades(trades, ex_name)
    all_trades = trades

    print(*map(to_datetime, [ftime, ltime]))
    while ltime < END_DT_TS:
        time.sleep(2.)
        trades = api.fetch_trades(symbol=sym, since=ltime - 10000, limit=100000)
        ftime, ltime, trades = process_trades(trades, ex_name)
        print(*map(to_datetime, [ftime, ltime]))

        all_trades.extend(trades)

    df = pd.DataFrame(all_trades)
    df = df.drop_duplicates()
    df['time'] = pd.to_datetime(df['timestamp'] // 1000, unit='s')
    print(df.describe())
    df.to_csv('data/extended_raw/{ex_name}_trades_{sym}.csv'.format(ex_name=ex_name, sym=sym.replace('/','-')), index=False)

def fetch_trades():
    th_list = [threading.Thread(target=fetch_ex, args=(api, sym)) for api, sym in exchanges]

    for t in th_list:
        t.start()

    for t in th_list:
        t.join()



if __name__ == "__main__":
    fetch_trades()

