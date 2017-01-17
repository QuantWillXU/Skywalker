from skywalker import bar
# from pyalgotrade import plotter
from skywalker import strategy
from skywalker.barfeed import pgfeed
from skywalker.barfeed import yahoofeed
from skywalker.barfeed import tusharefeed
from pyalgotrade.tools import yahoofinance
from skywalker.stratanalyzer import sharpe
from skywalker.technical import vwap
from skywalker import skysenseplotter

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, WeekdayLocator,\
    DayLocator, MONDAY,date2num,MonthLocator
from matplotlib.finance import quotes_historical_yahoo_ohlc, candlestick_ohlc


class VWAPMomentum(strategy.SkysenseBacktestingStrategy):
    def __init__(self):
        instrument = "600128.SH"
        vwapWindowSize = 6
        threshold = 0.01
        fromdate = "2015-09-01"
        todate = "2016-08-31"
        self.__benchmark = '000001.SH'
        feed = pgfeed.Feed()
        # feed.importBars([instrument,self.__benchmark], fromDateTime=fromdate, toDateTime=todate)
        feed.loadBars([instrument, self.__benchmark], fromDateTime=fromdate, toDateTime=todate)
        super(VWAPMomentum, self).__init__(feed)
        self.__instrument = instrument
        self.__vwap = vwap.VWAP(feed[instrument], vwapWindowSize)
        self.__threshold = threshold

    def getBenchmark(self):
        return self.__benchmark

    def getInstrument(self):
        return [self.__instrument]

    def getVWAP(self):
        return self.__vwap

    def onBars(self, bars):
        vwap = self.__vwap[-1]
        if vwap is None:
            return

        shares = self.getBroker().getShares(self.__instrument)
        price = bars[self.__instrument].getClose()
        notional = shares * price

        if price > vwap * (1 + self.__threshold) and notional < 1000000:
            self.marketOrder(self.__instrument, 1000)
        elif price < vwap * (1 - self.__threshold) and notional > 0:
            self.marketOrder(self.__instrument, -1000)


def main(plot):

    # feed = tusharefeed.Feed()
    # feed.loadBarsFromTushare(instruments=['600000'],fromdate="2015-08-10", todate="2016-08-24")
    strat = VWAPMomentum()
    sharpeRatioAnalyzer = sharpe.SharpeRatio()
    strat.attachAnalyzer(sharpeRatioAnalyzer)

    if plot:
        pass
        # plt = plotter.StrategyPlotter(strat, True, True, True)
        # plt.getInstrumentSubplot(instrument[0]).addDataSeries("vwap", strat.getVWAP())
        # plt.getOrCreateSubplot('Returns').addDataSeries("Strategy", strat.skysenseAnalyzer.getCumulativeReturns())
        # plt.getOrCreateSubplot('Returns').addCallback("Benchmark", lambda x: strat.skysenseAnalyzer.getBenchmarkCumuReturns()[strat.skysenseAnalyzer.getCumulativeReturns().getDateTimes()[0:].index(x.getDateTime())])
        # plt.getOrCreateSubplot('Returns_').addDataSeries("Benchmark", strat.skysenseAnalyzer.getBenchmarkCumuReturns())


    strat.run()
    # strat.skysenseAnalyzer.generateReport('sma.jpg')
    plter = skysenseplotter.StrategyPlotter(strat)

    plter.plotReturns()

    # print strat.skysenseAnalyzer.getAlpha()
    # print strat.skysenseAnalyzer.getInformationRatio()

    # date = strat.skysenseAnalyzer.getCumulativeReturns().getDateTimes()[0:].index(x.getDateTime())
    # ret = strat.skysenseAnalyzer.getCumulativeReturns()[strat.skysenseAnalyzer.getCumulativeReturns().getDateTimes()[0:].index(x.getDateTime())]
    # print ret

    barsList = strat.skysenseAnalyzer.getBarsList()
    dateList = strat.skysenseAnalyzer.getDateTimeList()
    result = []
    today = None
    yesterday = None
    for i in range(0,len(dateList)):
        dict = {}
        year = dateList[i].year
        month = dateList[i].month
        if i == 0:
            result.append([dateList[i]])
        elif dateList[i].month != dateList[i-1].month and i != len(dateList)-1:
            result[-1].append(dateList[i-1])
            result.append([dateList[i]])
        elif i == len(dateList)-1:
            if dateList[i].month != dateList[i-1].month:
                result[-1].append(dateList[i - 1])
                result.append([dateList[i], dateList[i]])
            else:
                result[-1].append(dateList[i])

    # print result

    quotes = []
    for i in range(0, len(barsList)):
        time = date2num(dateList[i])
        open = barsList[i].getBar("600128.SH").getOpen()
        close = barsList[i].getBar("600128.SH").getClose()
        high = barsList[i].getBar("600128.SH").getHigh()
        low = barsList[i].getBar("600128.SH").getLow()
        tup = (time, open, high, low, close)
        quotes.append(tup)

    quotes1 = []
    for i in range(0, len(barsList)):
        time = date2num(dateList[i])
        open = barsList[i].getBar('000001.SH').getOpen()
        close = barsList[i].getBar('000001.SH').getClose()
        high = barsList[i].getBar('000001.SH').getHigh()
        low = barsList[i].getBar('000001.SH').getLow()
        vol = barsList[i].getBar('000001.SH').getVolume()
        tup = (time, open, high, low, close, vol)
        quotes1.append(tup)

    mondays = WeekdayLocator(MONDAY)  # major ticks on the mondays
    alldays = DayLocator()  # minor ticks on the days

    fig = plt.figure()
    ax = fig.add_subplot(4,1,1)

    fig.subplots_adjust(bottom=0.2, hspace=0)
    candlestick_ohlc(ax, quotes, width=0.6)
    ax.xaxis_date()
    ax.autoscale_view()

    for label in ax.xaxis.get_ticklabels():
        label.set_rotation(45)

    ax.set_title("600128.SH")
    ax_benchmark = fig.add_subplot(4,1,3)
    candlestick_ohlc(ax_benchmark, quotes1, width=0.6)
    ax_benchmark.xaxis_date()
    ax_benchmark.autoscale_view()
    ax_benchmark.set_title("000001.SH")


    # ax.xaxis.grid(True)
    # ax.yaxis.grid(True)
    # ax.set_xticks([])
    ax_benchmark.set_xticks([])
    ax.grid(True)
    ax_return = fig.add_subplot(4,1,4)
    ax_return.plot(dateList,strat.skysenseAnalyzer.getCumulativeReturns()[0:],label="strat")
    ax_return.xaxis.set_major_locator(MonthLocator())
    ax_return.xaxis.set_major_formatter(DateFormatter("%Y-%m"))
    # ax_return.plot(dateList,strat.skysenseAnalyzer.getBenchmarkCumuReturns()[0:],label='benchmark')
    ax_return.legend(loc='best')
    plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
    # plt.setp(ax.get_xticklabels(), visible=False)
    y1 = ax.yaxis.get_ticklabels()[0]
    y2 = ax_benchmark.yaxis.get_ticklabels()[0]
    plt.setp(y1, visible=False)
    plt.setp(y2, visible=False)
    plt.show()

if __name__ == "__main__":
    main(True)