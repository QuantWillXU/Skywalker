import datetime

from skywalker import bar
from skywalker.barfeed import csvfeed
from skywalker.utils import dt


def parse_date(date):
    return datetime.datetime.strptime(date, "%Y/%m/%d")


def parse_date2(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d")


class EquityBar(bar.Bar):
    __slots__ = ('__dateTime', '__equity', '__frequency')

    def __init__(self, dateTime, equity, frequency=bar.Frequency.DAY):
        self.__dateTime = dateTime
        self.__equity = equity
        self.__frequency = frequency

    def __setstate__(self, state):
        (self.__dateTime, self.__equity) = state

    def __getstate__(self):
        return (self.__dateTime, self.__equity)

    def getEquity(self):
        return self.__equity

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted:
            raise Exception("Adjusted close is not available")

    def getFrequency(self):
        return self.__frequency

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        return self.__equity

    def getHigh(self, adjusted=False):
        return self.__equity

    def getLow(self, adjusted=False):
        return self.__equity

    def getClose(self, adjusted=False):
        return self.__equity

    def getVolume(self):
        return self.__equity

    def getAdjClose(self):
        return None

    def getPrice(self):
        return self.__equity

    def getUseAdjValue(self):
        return False


class EquityRowParser(csvfeed.RowParser):
    def __init__(self, dailyBarTime, frequency, timezone=None, sanitize=False, barClass=EquityBar):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize
        self.__barClass = barClass

    def __parseDate(self, dateString):
        # print dateString
        ret = parse_date2(dateString)
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def getFieldNames(self):
        return None

    def getDelimiter(self):
        return ","

    def parseBar(self, csvRowDict):
        dateTime = self.__parseDate(csvRowDict["Date"])
        equity = float(csvRowDict['Equity'])
        return self.__barClass(dateTime, equity, self.__frequency)


class EquityFeed(csvfeed.BarFeed):
    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=None):
        if isinstance(timezone, int):
            raise Exception(
                "timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if frequency not in [bar.Frequency.DAY, bar.Frequency.WEEK]:
            raise Exception("Invalid frequency.")

        super(EquityFeed, self).__init__(frequency, maxLen)

        self.__timezone = timezone
        self.__sanitizeBars = False
        self.__barClass = EquityBar

    def setBarClass(self, barClass):
        self.__barClass = barClass

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromCSV(self, instrument, path, timezone=None):
        if isinstance(timezone, int):
            raise Exception(
                "timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None:
            timezone = self.__timezone

        rowParser = EquityRowParser(
            self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars, self.__barClass
        )
        super(EquityFeed, self).addBarsFromCSV(instrument, path, rowParser)
