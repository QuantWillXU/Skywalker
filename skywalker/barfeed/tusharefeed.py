import os

import pandas as pd
import tushare as ts

from skywalker import bar
from skywalker.barfeed import yahoofeed


class Feed(yahoofeed.Feed):
    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=None):
        super(Feed, self).__init__(frequency=frequency, timezone=timezone, maxLen=maxLen)

    def loadBarsFromTushare(self, instruments, fromdate, todate, remove=True):
        for i in range(0, len(instruments)):
            print('Start downloading date of %s from %s to %s' % (instruments[i], fromdate, todate))
            data = ts.get_h_data(code=instruments[i], start=fromdate, end=todate, autype=None)
            data_hfq = ts.get_h_data(code=instruments[i], start=fromdate, end=todate, autype='hfq')
            bars = pd.DataFrame({'Open': data['open'], 'High': data['high'], 'Low': data['low'], 'Close': data['close'],
                                 'Volume': data['volume'], 'Adj Close': data_hfq['close']},
                                columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])
            bars.index.name = 'Date'
            csvName = instruments[i] + '_' + fromdate + '_' + todate + '.csv'
            bars.to_csv(csvName)
            self.addBarsFromCSV(instrument=instruments[i], path=csvName)
            if remove:
                os.remove(csvName)
