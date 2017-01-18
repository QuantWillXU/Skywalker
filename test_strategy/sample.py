from skywalker import strategy


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
        self.marketOrder(self.getInstruments()[0], self.count)
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


strat = MyStrategy()
strat.setStartDate("2016-01-01")
strat.setEndDate("2017-01-01")
strat.setUniverse(["000712.SZ"])
strat.setBenchmenk('002517.SZ')
strat.run(generateReport=False, haveStrat=True, show=True)

# strat.run()
# strat.getFeed()['002113.SZ'].getPandasDataFrame().to_csv('002113.SZ.csv')