from skywalker import strategy
import numpy as np


class MyStrategy(strategy.SkysenseBacktestingStrategy):
    def __init__(self):
        super(MyStrategy, self).__init__()

    def onStart(self):
        self.count = 0
        pass

    def onBars(self, bars):
        # if self.count % 5 == 0:
            # self.marketOrder('600128.SH',-500)
        # else:
        # if self.count <= 200:
        #     self.marketOrder('002113.SZ', 1000)
        # self.count += 100
        # self.getBroker().fixPositions('600128.SH', self.count)
        # print self.getCurrentDateTime()
        self.count += 1
        self.marketOrder(self.getInstruments()[0],self.count)
        # print(self.getInstruments())
        # print self.getFeed().getCurrentDateTime()

        # print bars.getDateTime()
        # print self.getFeed().haveEquityEvent(self.getInstruments()[0], bars.getDateTime())
        # print self.getFeed().getEquityEvent(self.getInstruments()[0])
        # print bars[self.getInstruments()[0]].getExtraColumns()
        # print self.getFeed()[self.getInstruments()[0]].getPandasDataFrame().ix[-1]
        # print self.getForwardAdjOpen(self.getInstruments()[0])[-1]
        # print bars[self.getInstruments()[0]].getOpen()
        # print self.getFeed().getEquityEvent(self.getInstruments()[0])
        # print self.getTradeStatus(self.getInstruments()[0])[-1]
        # print isnan(self.getFundamental(self.getInstruments()[0],'div_capitalization')[-1])
        # print self.getTradeStatus(self.getInstruments()[0])[-1]


# ["300345.SZ"],'2015-06-25','2016-07-30'
strat = MyStrategy()
strat.setStartDate("2000-06-26")
strat.setEndDate("2016-07-30")
strat.setUniverse(["000333.SZ"])
# strat.setUniverse(["300345"])
# strat.setUniverse()'002113.SZ','000005.SZ', '000025.SZ',
# strat.setBenchmenk('000002.SZ')
# strat.setBenchmenk('000001')
# strat.setUseAdjustedValues(True)
strat.run(generateReport=True, haveStrat=True, show=False)
# strat.run()
# strat.getFeed()['002113.SZ'].getPandasDataFrame().to_csv('002113.SZ.csv')