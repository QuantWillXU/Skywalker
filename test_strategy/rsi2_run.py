import rsi2
from skywalker import bar
from skywalker import plotter
from skywalker.barfeed import pgfeed


def main(plot):
    entrySMA = 200
    exitSMA = 5
    rsiPeriod = 2
    overBoughtThreshold = 90
    overSoldThreshold = 10
    instrument = '600001.SH'
    fromdate = "2013-08-10"
    todate = "2016-08-24"
    feed = pgfeed.Feed(frequency=bar.Frequency.DAY)
    feed.loadBars([instrument], fromDateTime=fromdate, toDateTime=todate)
    strat = rsi2.RSI2(feed, instrument, entrySMA, exitSMA, rsiPeriod, overBoughtThreshold, overSoldThreshold)
    if plot:
        plt = plotter.StrategyPlotter(strat, True, True, True)
    strat.run()
    if plot:
        plt.plot()



if __name__ == "__main__":
    main(True)
