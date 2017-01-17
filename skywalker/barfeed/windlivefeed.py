import datetime as dt
import time

from WindPy import *

from skywalker import bar
from skywalker import barfeed


class WindBar(bar.Bar):
    pass


class WindLiveFeed(barfeed.BaseBarFeed):
    def __init__(self, instruments=[], frequency=bar.Frequency.TRADE):
        super(WindLiveFeed, self).__init__(frequency)
        self.__instruments = instruments
        self.__currentDatetime = None
        self.__frequence = frequency
        self.__currentBars = None
        self.__startTime = dt.datetime.now()
        w.start()

    def getCurrentDateTime(self):
        return self.__currentDatetime

    def barsHaveAdjClose(self):
        False

    def getNextBars(self):
        preCurrentDT = self.__currentDatetime
        # while self.__currentDatetime is None or preCurrentDT is None or preCurrentDT >= self.__currentDatetime:
        data = w.wsq(self.__instruments, "rt_last,rt_last_vol")
        time.sleep(1)
        # preCurrentDT = self.__currentDatetime
        self.__currentDatetime = data.Times[0]

        barList = [bar.BasicBar(dateTime=self.__currentDatetime, open_=data.Data[0][i], close=data.Data[0][i],
                                high=data.Data[0][i], low=data.Data[0][i], adjClose=data.Data[0][i],
                                volume=data.Data[1][i], frequency=self.__frequence) for i in
                   range(len(self.__instruments))]
        bars = {key: value for key, value in zip(self.__instruments, barList)}
        self.__currentBars = bars

        return bar.Bars(bars)

    def eof(self):
        if dt.datetime.today().time() <= dt.time(hour=15):
            eof = False
        else:
            eof = True
            w.stop()
        return eof

    def start(self):
        super(WindLiveFeed, self).start()

    def stop(self):
        w.stop()
        pass

    def join(self):
        pass

    def peekDateTime(self):
        return None
