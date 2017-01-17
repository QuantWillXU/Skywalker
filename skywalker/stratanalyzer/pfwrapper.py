import pandas as pd


class PyfolioWrapper():
    def __init__(self, analyzer):
        self.__analyer = analyzer
        self.__analyzerTradesListDict = analyzer.getTradesListDict()
        self.__analyzerEquityList = analyzer.getEquityList()
        self.__analyzerCashList = analyzer.getCashList()
        self.__analyzerPositionsList = analyzer.getPositionsList()
        self.__analyzerBenchmarkList = analyzer.getBenchmarkList()
        self.__analyzerDateTimeList = analyzer.getDateTimeList()
        self.__analyzerbarsList = analyzer.getBarsList()
        self.__returns = None
        self.__benchmarkReturns = None
        self.__transactions = None
        self.__positions = None
        self.__shares = None
        self.__closes = None

    def getReturns(self):
        if self.__returns is None:
            self.__returns = pd.Series(self.__analyzerEquityList, index=self.__analyzerDateTimeList).pct_change()
        return self.__returns

    def getBenchmarkReturns(self):
        if self.__benchmarkReturns is None:
            self.__benchmarkReturns = pd.Series(self.__analyzerBenchmarkList,
                                                index=self.__analyzerDateTimeList).pct_change()
            self.__benchmarkReturns.name = 'Benchmark'
        return self.__benchmarkReturns

    def getTransactions(self):
        if self.__transactions is None:
            # date = map(lambda dateStr:datetime.strptime(dateStr,'%Y-%m-%d'),self.__analyzerTradesListDict['date'])
            datetime = self.__analyzerTradesListDict['datetime']

            amount = map(self.__amountWithSign, self.__analyzerTradesListDict['shares'],
                         self.__analyzerTradesListDict['type'])
            symbol = self.__analyzerTradesListDict['instrument']
            price = self.__analyzerTradesListDict['price']
            self.__transactions = pd.DataFrame(data=list(zip(amount, price, symbol)),
                                               columns=['amount', 'price', 'symbol'],
                                               index=datetime)
        return self.__transactions

    def __amountWithSign(self, amount, type):
        if type == 'BUY' or type == 'BUY_TO_COVER':
            return amount
        elif type == 'SELL' or type == 'SELL_SHORT':
            return amount * -1

    def getShares(self):
        if self.__shares is None:
            self.__shares = pd.DataFrame(self.__analyzerPositionsList, index=self.__analyzerDateTimeList).fillna(
                value=0)
        return self.__shares

    def getCloses(self):
        if self.__closes is None:
            closesDictList = [{instrument: bars[instrument].getClose() for instrument in bars.getInstruments()} for bars
                              in self.__analyzerbarsList]
            self.__closes = pd.DataFrame(data=closesDictList, index=self.__analyzerDateTimeList).ffill()
        return self.__closes

    def getPositions(self):
        if self.__positions is None:
            self.__positions = self.getShares() * self.getCloses()
            self.__positions['cash'] = self.__analyzerCashList
        return self.__positions

    def getALL(self):
        ret = {'returns': self.getReturns(), 'benchmark_ret': self.getBenchmarkReturns(),
               'transactions': self.getTransactions(), 'positions': self.getPositions()}
        return ret

    def generateReport(self):
        import os
        import skywalker.pyfolio as pf
        dir = 'Performance Analysis'
        childDirs = ['Return', 'Positions', 'TNX', 'Round Trip']
        childDirs = ['/'.join([dir, childDir]) for childDir in childDirs]
        try:
            [os.makedirs(dir) for dir in childDirs]
        except OSError:
            pass
        perf_stats, drawdown_df = pf.create_returns_tear_sheet(returns=self.getReturns(),
                                                               benchmark_rets=self.getBenchmarkReturns(),
                                                               save_fig=childDirs[0] + '/returns')
        perf_stats.to_excel(childDirs[0] + '/Performance statistics.xlsx')
        drawdown_df.to_excel(childDirs[0] + '/Drawdown.xlsx')
        df_top_long, df_top_short, df_top_abs = pf.create_position_tear_sheet(returns=self.getReturns(),
                                                                              positions=self.getPositions(),
                                                                              save_fig=childDirs[1] + '/positions')
        pd.DataFrame(df_top_long, columns=['Exposures']).to_excel(childDirs[1] + '/Top Long Positions.xlsx')
        pd.DataFrame(df_top_short, columns=['Exposures']).to_excel(childDirs[1] + '/Top Short Positions.xlsx')
        pd.DataFrame(df_top_abs, columns=['Exposures']).to_excel(childDirs[1] + '/Top Positions.xlsx')
        pf.tears.create_txn_tear_sheet(returns=self.getReturns(), positions=self.getPositions(),
                                       transactions=self.getTransactions(), save_fig=childDirs[2] + '/txn')
        stats, pct_profit_attribution = pf.create_round_trip_tear_sheet(returns=self.getReturns(),
                                                                        positions=self.getPositions(),
                                                                        transactions=self.getTransactions(),
                                                                        save_fig=childDirs[3] + '/round trip')
        for k, v in stats.items():
            v.to_excel(childDirs[3] + '/' + k + '.xlsx')
        pd.DataFrame(pct_profit_attribution, columns=['pnl']).to_excel(
            childDirs[3] + '/' + 'pct_profit_attribution.xlsx')
