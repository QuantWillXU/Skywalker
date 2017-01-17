import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, date2num, MonthLocator
from matplotlib.finance import candlestick_ohlc


class StrategyPlotter(object):
    def __init__(self, strat):
        self.__strat = strat

    def plotReturns(self, withStrat=True, show=False):
        strat = self.__strat
        fig = plt.figure()
        ax_return = fig.add_subplot(2, 1, 1)
        dateList = strat.skysenseAnalyzer.getDateTimeList()
        returns = strat.skysenseAnalyzer.getCumulativeReturns()[0:]
        assert len(dateList) == len(returns)
        ax_return.plot(dateList, returns, label="Strategy")
        if withStrat:
            ax_return.plot(dateList, strat.skysenseAnalyzer.getBenchmarkCumuReturns()[0:], label='Benchmark')
        ax_return.grid(True)
        ax_return.set_title('Returns')
        ax_return.legend(loc='best')
        plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
        plt.savefig('returns.jpg')
        if show:
            plt.show()

    def plotInstruments(self):
        strat = self.__strat
        instrument = strat.getInstruments()[0]
        barsList = strat.skysenseAnalyzer.getBarsList()
        dateList = strat.skysenseAnalyzer.getDateTimeList()
        volumeList = []
        for i in range(0, len(barsList)):
            volumeList.append(barsList[i].getBar(instrument).getVolume())
        fig = plt.figure()
        fig.subplots_adjust(bottom=0.2, hspace=0)
        ax_instrument = plt.subplot2grid((4, 4), (0, 0), rowspan=3, colspan=4)
        quotes = []
        for i in range(0, len(barsList)):
            time = date2num(dateList[i])
            open = barsList[i].getBar(instrument).getOpen()
            close = barsList[i].getBar(instrument).getClose()
            high = barsList[i].getBar(instrument).getHigh()
            low = barsList[i].getBar(instrument).getLow()
            tup = (time, open, high, low, close)
            quotes.append(tup)
        candlestick_ohlc(ax_instrument, quotes, width=0.6)
        ax_instrument.xaxis_date()
        ax_instrument.autoscale_view()
        ax_instrument.set_ylabel('Candlestick Charts')
        ax_instrument.grid(True)
        ax_volume = plt.subplot2grid((4, 4), (3, 0), colspan=4)
        ax_volume.bar(dateList, [vol / 10000 for vol in volumeList])
        ax_volume.set_ylabel("Volume(ten thousand shares)")
        ax_volume.xaxis.set_major_locator(MonthLocator())
        ax_volume.xaxis.set_major_formatter(DateFormatter("%Y-%m"))
        ax_volume.grid(True)
        plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
        plt.setp(ax_instrument.get_xticklabels(), visible=False)
        plt.setp(ax_instrument.get_yticklabels()[0], visible=False)
        plt.setp(ax_volume.get_xticklabels()[0], visible=False)
        plt.show()
