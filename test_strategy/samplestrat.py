from skywalker import strategy
from skywalker.technical import vwap
from skywalker.talibext.indicator import *
from skywalker.stratanalyzer import pfwrapper

class VWAPMomentum(strategy.SkysenseBacktestingStrategy):
    def __init__(self):
        super(VWAPMomentum, self).__init__()

    def onStart(self):
        self.__vwap = vwap.VWAP(self.getFeed()[self.getInstruments()[0]], 6)
        self.__count = 0

    def onBars(self, bars):
        self.__count += 1
        if self.getEquity() < self.getInitialCaptial()*0.9:
            return

        try:
            benchmark = bars[self.getBenchmark()].getClose()
            lastBenchmenk = self.getLastPrice(self.getBenchmark())
            lastClose = self.getLastPrice(self.getInstruments()[0])
            close = bars[self.getInstruments()[0]].getClose()
            if (benchmark - lastBenchmenk) / lastBenchmenk > (close - lastClose) / lastClose:
                self.marketOrder(self.getInstruments()[0], -1000)
            else:
                self.marketOrder(self.getInstruments()[0], 3000)
        except KeyError:
            pass

        try:
            if self.__count % 20 == 0:
                ups = {}
                for instrument in self.getInstruments():
                    close = self.getClose(instrument)
                    ups[instrument] = (close[-1]-close[-20])/close[-20]
                maxInstrument = None
                maxUp = None
                for key in ups.keys():
                    if maxInstrument is None or ups[key] > maxUp:
                        maxInstrument = key
                        maxUp = ups[key]
                self.marketOrder(maxInstrument, 500)
                others = list(ups.keys())
                others.remove(maxInstrument)
                for instrument in others:
                    self.marketOrder(instrument, 500.0/len(others))
        except IndexError:
            pass

        vwap = self.__vwap[-1]
        if vwap is None:
            return
        price = bars[self.getInstruments()[0]].getClose()
        if price > vwap:
            self.marketOrder(self.getInstruments()[0], 100)
        elif price < vwap:
            self.marketOrder(self.getInstruments()[0], -100)

        upper, middle, lower = BBANDS(self.getClose(self.getInstruments()[0]), 100)
        lastClose = self.getLastPrice(self.getInstruments()[0])
        if lastClose is None:
            return
        if lastClose > upper[-1]:
            self.marketOrder(self.getInstruments()[0], -100)
        if lastClose < lower[-1]:
            self.marketOrder(self.getInstruments()[0], 100)

        if self.__count % 7 == 0:
            for pos in self.getActivePositions():
                pos.exitMarket()
        else:
            for instrument in self.getInstruments():
                self.enterLong(instrument, 5000)




        # self.enterLong(self.getInstruments()[0], 5000)
        # print self.getFeed()[self.getInstruments()[0]]
        # print self.getBroker().get
        # self.getActivePositions()[-1]
        # print self.getBenchmark()
        # print self.getInstruments()
        # print self.getInitialCaptial()
        # print self.getShares(self.getInstruments()[0])
        # print self.getPositions()
        # print self.getCash()
        # print self.getEquity()
        # print self.getAdjClose(self.getInstruments()[0])[-1]
        # print bars[self.getInstruments()[0]].getClose()
        # print self.getClose(self.getInstruments()[0])[0]
        # print self.getTradeStatus(self.getInstruments()[0])[-1]
        # print self.getActivePositions()
        # print self.getLastPrice(self.getInstruments()[0])
        # print self.getFeed()
        # print self.getBroker()
        # print self.getCurrentDateTime()
        # print bars.getDateTime()


if __name__ == "__main__":
    strat = VWAPMomentum()
    strat.setStartDate("2015-09-01")
    strat.setEndDate("2016-08-30")
    strat.setUniverse(['000005.SZ', '000025.SZ', '600128.SH'])
    # strat.setUniverse()
    # strat.setUniverse(["000001.SZ"])
    strat.setBenchmenk('000001.SH')
    strat.setUseAdjustedValues(True)
    strat.run(False)
    wrapper = pfwrapper.PyfolioWrapper(strat.skysenseAnalyzer)
    returns = wrapper.getReturns()
    benchmarkReturns = wrapper.getBenchmarkReturns()
    positions = wrapper.getPositions()
    transactions = wrapper.getTransactions()
    wrapper.generateReport()
    from skywalker.pyfolio.plotting import *
    import skywalker.pyfolio as pf
    # pf.create_returns_tear_sheet(returns=returns,benchmark_rets=benchmarkReturns,save_fig='return')
    # pf.create_position_tear_sheet(returns=returns,positions=positions,save_fig='positions')
    # pf.tears.create_txn_tear_sheet(returns=returns,positions=positions,transactions=transactions,save_fig='txn')
    # pf.create_round_trip_tear_sheet(returns=returns,positions=positions,transactions=transactions,save_fig='round_trip')
    # pf.create_bayesian_tear_sheet(returns=returns,benchmark_rets=benchmarkReturns,live_start_date=returns.index[-40],save_fig='bayesian')
    # import matplotlib.pyplot as plt
    # fig = plt.figure()
    # ax = fig.add_subplot(111)
    # plot_annual_returns(returns,ax=ax)
    # plot_holdings(returns,positions,ax=ax)
    # perf_stats = show_perf_stats(returns,benchmarkReturns)
    # print(perf_stats)

    # plt.show()
    #
    # from reportlab.lib.units import inch, cm
    # from reportlab.lib.pagesizes import A4
    # from reportlab.pdfgen import canvas
    #
    #
    # def drawPageFrame(canv):
    #     canv.line(left_margin, top_margin, right_margin, top_margin)
    #     canv.setFont('Times-Italic', 12)
    #     canv.drawString(left_margin, top_margin + 2, "Homer's Odyssey")
    #     canv.line(left_margin, top_margin, right_margin, top_margin)
    #
    #     canv.line(left_margin, bottom_margin, right_margin, bottom_margin)
    #     canv.drawCentredString(0.5 * A4[0], 0.5 * inch,
    #                            "Page %d" % canv.getPageNumber())
    #
    #
    # # precalculate some basics
    # top_margin = A4[1] - inch
    # bottom_margin = inch
    # left_margin = inch
    # right_margin = A4[0] - inch
    # frame_width = right_margin - left_margin
    # canv = canvas.Canvas('test.pdf', invariant=1)
    # tx = canv.beginText(left_margin, top_margin-inch)
    # for text in perf_stats.index:
    #     tx.textLine(text)
    # canv.drawText(tx)
    # tx = canv.beginText(left_margin+inch*3, top_margin-inch)
    # for text in perf_stats[perf_stats.columns[0]]:
    #     tx.textLine(str(text))
    # canv.drawText(tx)
    # drawPageFrame(canv)
    # canv.line(left_margin, top_margin-0.5*inch, right_margin, top_margin-0.5*inch)
    # canv.setFont('Times-Italic', 12)
    # canv.drawString(left_margin, top_margin-0.4*inch, "Performance Statistics")
    # canv.showPage()
    # canv.save()


