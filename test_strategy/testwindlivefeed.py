from pyalgotrade import strategy

from skywalker.barfeed import windlivefeed
from skywalker.broker import windbroker


class TestStrat(strategy.BaseStrategy):
    def __init__(self, feed, brk):
        super(TestStrat, self).__init__(feed, brk)


    def onBars(self, bars):
        # print bars["600900.SH"].getOpen()
        # print self.getCurrentDateTime()
        # print self.getActivePositions()
        # print self.getLastPrice("600900.SH")
        print(self.getBroker().getPositions())
        # print self.getBroker().getCash()
        # print self.getBroker().getEquity()
        self.limitOrder(instrument="300345.SZ",limitPrice=11,quantity=100)
        # self.marketOrder("300345.SZ", -100)
        print(self.getCurrentDateTime())
        # print self.getBroker().getActiveOrders()



def main(plot):
    feed = windlivefeed.WindLiveFeed(instruments=["600900.SH"])
    brk = windbroker.WindBroker(feed)
    strat = TestStrat(feed, brk)
    strat.run()


if __name__ == "__main__":
    main(True)