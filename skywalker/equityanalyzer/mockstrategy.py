from skywalker import strategy
from skywalker.equityanalyzer import analyzer
from skywalker.equityanalyzer import equityfeed


class MockStrategy(strategy.SkysenseBacktestingStrategy):
    def __init__(self, cash_or_brk=14.6, feed=equityfeed.EquityFeed(), analyzer=analyzer.analyzer_skysense()):
        super(MockStrategy, self).__init__(cash_or_brk=cash_or_brk, feed=feed, analyzer=analyzer)

    def loadData(self):
        all = self.getUniverse()
        start = self.getStartDate()
        end = self.getEndDate()
        benchmark = self.getBenchmark()
        if benchmark in all:
            all.remove(benchmark)
        all.append(benchmark)
        # self.getFeed().loadBarsFromTushare(instruments=all, fromdate=start, todate=end, remove=False)
        # self.getFeed().addBarsFromCSV(instrument='300345', path='300345_2015-06-26_2016-07-30.csv')
        # self.getFeed().addBarsFromCSV(instrument='000001', path='000001_2015-06-26_2016-07-30.csv')
        self.getFeed().addBarsFromCSV(instrument='equity2', path='data2.csv')
        self.getFeed().addBarsFromCSV(instrument='benchmark', path='data2.csv')


class TestStrat(MockStrategy):
    def __init__(self, cash_or_brk=100000, feed=equityfeed.EquityFeed(), analyzer=analyzer.analyzer_skysense()):
        super(TestStrat, self).__init__(cash_or_brk=cash_or_brk, feed=feed, analyzer=analyzer)

    def onBars(self, bars):
        # print self.getCurrentDateTime()

        try:
            equity = bars['equity2'].getOpen()
            print
            equity
            self.getBroker().setCash(equity)
        except KeyError:
            pass

    def __onBars(self, dateTime, bars):
        print
        1
        pass


if __name__ == '__main__':
    strat = TestStrat(cash_or_brk=200324.57 - 40478.90)
    strat.setStartDate("201-04-29")
    strat.setEndDate("2014-05-27")
    strat.setUniverse(["equity2"])
    strat.setBenchmenk('benchmark')
    strat.run(generateReport=True, haveStrat=False)
