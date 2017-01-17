# -*- coding:utf-8 -*-
import datetime

import numpy as np
import pandas as pd
import statsmodels.api as sm

from skywalker import broker
from skywalker import dataseries
from skywalker.stratanalyzer import drawdown
from skywalker.stratanalyzer import excelreport
from skywalker.stratanalyzer import returns
from skywalker.stratanalyzer import sharpe
from skywalker.stratanalyzer import trades


class analyzer_skysense(sharpe.SharpeRatio, drawdown.DrawDown, returns.Returns, trades.Trades):
    """继承了pyalgotrade的所有analyzer，并添加了策略回测绩效报告中需要的功能"""

    def __init__(self, maxLen=None):
        super(analyzer_skysense, self).__init__()
        self.__netReturns = dataseries.SequenceDataSeries(maxLen=maxLen)
        self.__cumReturns = dataseries.SequenceDataSeries(maxLen=maxLen)
        self.__benchmarkDailyReturns = dataseries.SequenceDataSeries(maxLen=maxLen)
        self.__benchmarkCumuReturns = dataseries.SequenceDataSeries(maxLen=maxLen)
        self.__benchmarkList = []
        self.__dailyPeriodAnalysis = {'Date': [], 'profit': [], 'profitratio': []}
        self.__presentEquity = None
        self.__lastEquity = None
        self.__presentBenchmark = None
        self.__lastBenchmark = None
        self.__strat = None
        self.__totalSlippage = 0
        self.__momentPrice = 0
        self.__actionList = ["BUY", "BUY_TO_COVER", "SELL", "SELL_SHORT"]
        self.__tradesListDict = {'type': [],
                                 'date': [],
                                 'price': [],
                                 'shares': [],
                                 'return': [],
                                 'returnrate': [],
                                 'instrument': [],
                                 'position': [],
                                 'cost': []
                                 }
        self.__long = 0
        self.__short = 0
        self.__countTrades = 0
        self.__positionsList = []
        self.__dateTimeList = []
        self.__equityList = []
        self.__cashList = []
        self.__barsList = []
        self.__buyPricesList = []
        self.__buySharesList = []
        self.__sellShortPricesList = []
        self.__sellShortSharesList = []
        self.__holdingCostList = []
        self.__buyPricesDict = {}
        self.__buySharesDict = {}
        self.__sellShortPricesDict = {}
        self.__sellShortSharesDict = {}
        self.__positionsListByOrder = [{}]

    def __onOrderEvent(self, broker_, orderEvent):
        if orderEvent.getEventType() not in (broker.OrderEvent.Type.PARTIALLY_FILLED, broker.OrderEvent.Type.FILLED):
            return
        order = orderEvent.getOrder()
        execInfo = orderEvent.getEventInfo()
        pos = broker_.getPositions()
        positions = {}
        for k, v in pos.iteritems():
            positions[k] = v
        prePostitions = self.__positionsListByOrder[-1]
        fillPrice = execInfo.getPrice()
        shares = order.getFilled()
        instrument = order.getInstrument()
        if instrument not in self.__buyPricesDict.keys():
            self.__buyPricesDict[instrument] = []
        if instrument not in self.__buySharesDict.keys():
            self.__buySharesDict[instrument] = []
        if instrument not in self.__sellShortPricesDict.keys():
            self.__sellShortPricesDict[instrument] = []
        if instrument not in self.__sellShortSharesDict.keys():
            self.__sellShortSharesDict[instrument] = []

        if order.getAction() == 1 and shares != 0:  # buy
            if instrument not in prePostitions.keys() or prePostitions[instrument] >= 0:  # 当前仓位为零或为正
                # print '当前仓位为零或为正'
                self.__tradesListDict['return'].append('None')
                self.__tradesListDict['returnrate'].append('None')
                self.__buyPricesDict[instrument].append(fillPrice)
                self.__buySharesDict[instrument].append(shares)
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__buyPricesDict[instrument],
                                                                shareslist=self.__buySharesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['shares'].append(order.getFilled())
            elif -prePostitions[instrument] >= shares:
                # 空单部分平仓
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__sellShortPricesDict[instrument],
                                                                shareslist=self.__sellShortSharesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['return'].append(-(fillPrice - avgHoldingCost) * shares)
                self.__tradesListDict['returnrate'].append(-(fillPrice - avgHoldingCost) / avgHoldingCost)
                self.__sellShortPricesDict[instrument].append(avgHoldingCost)
                self.__sellShortSharesDict[instrument].append(-shares)
                self.__tradesListDict['shares'].append(order.getFilled())

            elif -prePostitions[instrument] < shares:
                # print shares
                # 所有空单平仓
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__sellShortPricesDict[instrument],
                                                                shareslist=self.__sellShortSharesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['return'].append(-(fillPrice - avgHoldingCost) * (-prePostitions[instrument]))
                self.__tradesListDict['returnrate'].append(-(fillPrice - avgHoldingCost) / avgHoldingCost)
                self.__sellShortPricesDict[instrument].append(avgHoldingCost)
                self.__sellShortSharesDict[instrument].append(prePostitions[instrument])
                self.__tradesListDict['type'].append(self.__actionList[order.getAction() - 1])
                self.__tradesListDict['date'].append(execInfo.getDateTime().strftime('%Y-%m-%d'))
                self.__tradesListDict['price'].append(execInfo.getPrice())
                self.__tradesListDict['shares'].append(-prePostitions[instrument])
                self.__tradesListDict['instrument'].append(instrument)
                if instrument not in positions.keys():
                    self.__tradesListDict['position'].append(0)
                else:
                    self.__tradesListDict['position'].append(0)
                # 反手做多
                self.__tradesListDict['return'].append('None')
                self.__tradesListDict['returnrate'].append('None')
                self.__buyPricesDict[instrument].append(fillPrice)
                self.__buySharesDict[instrument].append(shares - prePostitions[instrument])
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__buyPricesDict[instrument],
                                                                shareslist=self.__buySharesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['shares'].append(shares - prePostitions[instrument])

        elif order.getAction() == 4 and shares != 0:  # sell short
            self.__sellShortPricesDict[instrument].append(fillPrice)
            self.__sellShortSharesDict[instrument].append(shares)
            self.__tradesListDict['return'].append('None')
            self.__tradesListDict['returnrate'].append('None')
            avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__sellShortPricesDict[instrument],
                                                            shareslist=self.__sellShortPricesDict[instrument])
            self.__tradesListDict['cost'].append(avgHoldingCost)
            self.__tradesListDict['shares'].append(order.getFilled())

        elif order.getAction() == 2 and shares != 0:  # buy to covrt
            avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__sellShortPricesDict[instrument],
                                                            shareslist=self.__sellShortSharesDict[instrument])
            self.__tradesListDict['cost'].append(avgHoldingCost)
            self.__tradesListDict['return'].append(-(fillPrice - avgHoldingCost) * shares)
            self.__tradesListDict['returnrate'].append(-(fillPrice - avgHoldingCost) / avgHoldingCost)
            self.__sellShortPricesDict[instrument].append(avgHoldingCost)
            self.__sellShortSharesDict[instrument].append(-shares)
            self.__tradesListDict['shares'].append(order.getFilled())

        elif order.getAction() == 3 and shares != 0:  # sell
            if instrument not in prePostitions.keys() or prePostitions[instrument] <= 0:
                self.__sellShortPricesDict[instrument].append(fillPrice)
                self.__sellShortSharesDict[instrument].append(shares)
                self.__tradesListDict['return'].append('None')
                self.__tradesListDict['returnrate'].append('None')
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__sellShortPricesDict[instrument],
                                                                shareslist=self.__sellShortPricesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['shares'].append(order.getFilled())

            elif prePostitions[instrument] < shares and prePostitions[instrument] > 0:

                # 所有多单平仓
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__buyPricesDict[instrument],
                                                                shareslist=self.__buySharesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                # print (fillPrice - avgHoldingCost) * prePostitions[instrument]
                self.__tradesListDict['return'].append((fillPrice - avgHoldingCost) * prePostitions[instrument])
                self.__tradesListDict['returnrate'].append((fillPrice - avgHoldingCost) / avgHoldingCost)
                self.__buyPricesDict[instrument].append(avgHoldingCost)
                self.__buySharesDict[instrument].append(-prePostitions[instrument])

                self.__tradesListDict['type'].append(self.__actionList[order.getAction() - 1])
                self.__tradesListDict['date'].append(execInfo.getDateTime().strftime('%Y-%m-%d'))
                self.__tradesListDict['price'].append(execInfo.getPrice())
                self.__tradesListDict['shares'].append(prePostitions[instrument])
                self.__tradesListDict['instrument'].append(instrument)
                if instrument not in positions.keys():
                    self.__tradesListDict['position'].append(0)
                else:
                    self.__tradesListDict['position'].append(0)
                # 反手做空
                self.__sellShortPricesDict[instrument].append(fillPrice)

                self.__sellShortSharesDict[instrument].append(shares - prePostitions[instrument])
                self.__tradesListDict['return'].append('None')
                self.__tradesListDict['returnrate'].append('None')
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__sellShortPricesDict[instrument],
                                                                shareslist=self.__sellShortPricesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['shares'].append(shares - prePostitions[instrument])

            elif prePostitions[instrument] >= shares:
                avgHoldingCost = self.__calculateAvgHoldingCost(pricelist=self.__buyPricesDict[instrument],
                                                                shareslist=self.__buySharesDict[instrument])
                self.__tradesListDict['cost'].append(avgHoldingCost)
                self.__tradesListDict['return'].append((fillPrice - avgHoldingCost) * shares)
                self.__tradesListDict['returnrate'].append((fillPrice - avgHoldingCost) / avgHoldingCost)
                self.__buyPricesDict[instrument].append(avgHoldingCost)
                self.__buySharesDict[instrument].append(-shares)
                self.__tradesListDict['shares'].append(order.getFilled())

        self.__tradesListDict['type'].append(self.__actionList[order.getAction() - 1])
        self.__tradesListDict['date'].append(execInfo.getDateTime().strftime('%Y-%m-%d'))
        self.__tradesListDict['price'].append(execInfo.getPrice())
        # self.__tradesListDict['shares'].append(order.getFilled())
        self.__tradesListDict['instrument'].append(instrument)
        if instrument not in positions.keys():
            self.__tradesListDict['position'].append(0)
        else:
            self.__tradesListDict['position'].append(positions[instrument])
        # 获取发生交易时的股价用于计算滑价
        index = self.__strat.getFeed()[order.getInstrument()].getCloseDataSeries().getDateTimes().index(
            execInfo.getDateTime())
        if order.getFillOnClose():
            self.__momentPrice = self.__strat.getFeed()[order.getInstrument()].getCloseDataSeries()[index]
        else:
            self.__momentPrice = self.__strat.getFeed()[order.getInstrument()].getOpenDataSeries()[index]
        self.__totalSlippage = self.__totalSlippage + broker_.getFillStrategy().getSlippageModel().getSlippagePerShare() * order.getFilled()
        self.__positionsListByOrder.append(positions)

    def beforeAttach(self, strat):
        super(analyzer_skysense, self).beforeAttach(strat)
        analyzer = returns.ReturnsAnalyzerBase.getOrCreateShared(strat)
        analyzer.getEvent().subscribe(self.__onReturns)
        self.__strat = strat

    def attached(self, strat):
        super(analyzer_skysense, self).attached(strat)
        strat.getBroker().getOrderUpdatedEvent().subscribe(self.__onOrderEvent)

    def beforeOnBars(self, strat, bars):
        super(analyzer_skysense, self).beforeOnBars(strat, bars)

        broker = strat.getBroker()
        broker.fixPositions(strat.getInstruments()[0], 10000)
        # 周期分析
        if self.__presentEquity is None and self.__lastEquity is None:
            self.__lastEquity = strat.getEquity()
            self.__presentEquity = strat.getEquity()
        else:
            self.__lastEquity = self.__presentEquity
            self.__presentEquity = strat.getEquity()
        self.__dailyPeriodAnalysis['Date'].append(bars.getDateTime().strftime('%Y-%m-%d'))
        self.__dailyPeriodAnalysis['profit'].append(self.__presentEquity - self.__lastEquity)
        self.__dailyPeriodAnalysis['profitratio'].append((self.__presentEquity - self.__lastEquity) / self.__lastEquity)
        # 生成时间序列和每日仓位列表
        pos = strat.getBroker().getPositions()
        # 使用strat.getBroker().getPositions()获取的仓位字典存在一些问题，重新生成一个相同内容的字典后将其加入仓位列表中
        dic = {}
        for k, v in pos.iteritems():
            dic[k] = v
        self.__positionsList.append(dic)
        self.__dateTimeList.append(bars.getDateTime())
        self.__equityList.append(broker.getEquity())
        self.__cashList.append(broker.getCash())
        self.__barsList.append(bars)
        if bars.getBar(strat.getBenchmark()) is not None:
            self.__lastBenchmark = self.__presentBenchmark
            self.__presentBenchmark = bars.getBar(strat.getBenchmark()).getClose()
        if self.__lastBenchmark is None:
            self.__benchmarkDailyReturns.appendWithDateTime(bars.getDateTime(), 0)
        else:
            self.__benchmarkDailyReturns.appendWithDateTime(bars.getDateTime(), (
            self.__presentBenchmark - self.__lastBenchmark) / self.__lastBenchmark)
        self.__benchmarkList.append(self.__presentBenchmark)
        self.__report = None

    def __calculateAvgHoldingCost(self, pricelist, shareslist):
        """计算平均持股成本"""
        assert len(pricelist) == len(shareslist)
        totalPrices = 0
        totalShares = 0
        for i in range(0, len(pricelist)):
            totalPrices = totalPrices + pricelist[i] * shareslist[i]
            totalShares = totalShares + shareslist[i]
        return totalPrices / totalShares

    def getCount(self):
        """Returns the total number of trades."""
        count = [i for i in self.__tradesListDict["return"] if i != 'None']
        return len(count)

    def getProfitableCount(self):
        """Returns the number of profitable trades."""
        profitableCount = [i for i in self.__tradesListDict["return"] if i > 0 and i != 'None']
        return len(profitableCount)

    def getUnprofitableCount(self):
        """Returns the number of unprofitable trades."""
        unprofitableCount = [i for i in self.__tradesListDict["return"] if i < 0 and i != 'None']
        return len(unprofitableCount)

    def getEvenCount(self):
        """Returns the number of trades whose net profit was 0."""
        evenCount = [i for i in self.__tradesListDict["return"] if i == 0]
        return len(evenCount)

    def getAll(self):
        """Returns a numpy.array with the profits/losses for each trade."""
        all = [i for i in self.__tradesListDict["return"] if i != 'None']
        return np.asarray(all)

    def getProfits(self):
        """Returns a numpy.array with the profits for each profitable trade."""
        profits = [i for i in self.__tradesListDict["return"] if i != 'None' and i > 0]
        if len(profits) == 0:
            return np.asarray([0, 0])
        else:
            return np.asarray(profits)

    def getMaxProfit(self):
        """最大盈利"""
        max = self.getProfits().max()
        return max

    def getMaxLosses(self):
        """最大亏损"""
        losses = self.getLosses()
        if len(losses) != 0:
            return losses.min()
        else:
            return 0

    def getLosses(self):
        """Returns a numpy.array with the losses for each unprofitable trade."""
        losses = [i for i in self.__tradesListDict["return"] if i != 'None' and i < 0]
        return np.asarray(losses)

    def getAllReturns(self):
        """Returns a numpy.array with the returns for each trade."""
        allReturns = [i for i in self.__tradesListDict["returnrate"] if i != 'None']
        return np.asarray(allReturns)

    def getPositiveReturns(self):
        """Returns a numpy.array with the positive returns for each trade."""
        positiveReturns = [i for i in self.__tradesListDict["returnrate"] if i != 'None' and i > 0]
        return np.asarray(positiveReturns)

    def getNegativeReturns(self):
        """Returns a numpy.array with the negative returns for each trade."""
        negativeReturns = [i for i in self.__tradesListDict["returnrate"] if i != 'None' and i < 0]
        return np.asarray(negativeReturns)

    def getDailyPeriodAnalysis(self):
        """策略回测绩效报告中的周期分析"""
        return self.__dailyPeriodAnalysis

    def getWinningRatio(self):
        """胜率"""
        if self.getCount() == 0:
            return 0
        else:
            return float(self.getProfitableCount()) / self.getCount()

    def getAverageProfit(self):
        """每次盈利的交易的平均盈利"""
        if len(self.getProfits()) == 0:
            return 0
        else:
            return self.getProfits().mean()

    def getAverageLoss(self):
        """每次亏损的交易的平均亏损"""
        if len(self.getLosses()) == 0:
            return 0
        else:
            return self.getLosses().mean()

    def getAvarageProfitLossRatio(self):
        """获取平均获利/平均亏损"""
        if self.getAverageLoss() == 0:
            return 'None'
        else:
            return self.getAverageProfit() / self.getAverageLoss()

    def getPaidSlippage(self):
        """已付滑价"""
        return self.__totalSlippage

    def __onReturns(self, dateTime, returnsAnalyzerBase):
        self.__netReturns.appendWithDateTime(dateTime, returnsAnalyzerBase.getNetReturn())
        self.__cumReturns.appendWithDateTime(dateTime, returnsAnalyzerBase.getCumulativeReturn())

    def getReturns(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the returns for each bar."""
        return self.__netReturns

    def getCumulativeReturns(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the cumulative returns for each bar."""
        return self.__cumReturns

    def getTradePeriod(self):
        """交易周期"""
        return self.__dateTimeList[-1] - self.__dateTimeList[0]

    def getStratRunTime(self):
        """策略运行时间"""
        ret = self.getTradePeriod() - self.getTotalEmptyDuration()
        return ret

    def getTotalEmptyDuration(self):
        """空仓总时间"""
        emptyTime = datetime.timedelta()
        for et in self.getEmptyDurationDict()['duration']:
            emptyTime = emptyTime + et
        return emptyTime

    def getLongestEmptyDuration(self):
        """最长空仓期"""
        return max(self.getEmptyDurationDict()['duration'])

    def getAnnualizedReturn(self):
        """年化收益率"""
        tradeDays = 250
        return (self.getCumulativeReturns()[-1] + 1) ** (float(tradeDays) / self.getTradePeriod().days) - 1

    def getTradesListDict(self):
        """交易列表"""
        return self.__tradesListDict

    def getPositionsList(self):
        """每日仓位列表"""
        return self.__positionsList

    def getDateTimeList(self):
        """日期列表"""
        return self.__dateTimeList

    def getEquityList(self):
        """每日权益列表"""
        return self.__equityList

    def getCashList(self):
        """每日现金列表"""
        return self.__cashList

    def getBarsList(self):
        """每日k线"""
        return self.__barsList

    def getEmptyDurationDict(self):
        """空仓期字典，包含空仓开始时间，结束时间和空仓期"""
        ret = {'start': [], 'end': [], 'duration': []}
        for i in range(0, len(self.__dateTimeList)):
            if i == 0:
                if not self.__positionsList[i].keys():
                    ret['start'].append(self.__dateTimeList[i])
            elif i == len(self.__dateTimeList) - 1:
                if not self.__positionsList[i].keys() and self.__positionsList[i - 1].keys():
                    ret['start'].append(self.__dateTimeList[i])
                    ret['end'].append(self.__dateTimeList[i] + datetime.timedelta(days=1))
                    ret['duration'].append(ret['end'][-1] - ret['start'][-1])
                elif not self.__positionsList[i].keys() and not self.__positionsList[i - 1].keys():
                    ret['end'].append(self.__dateTimeList[i] + datetime.timedelta(days=1))
                    ret['duration'].append(ret['end'][-1] - ret['start'][-1])
                elif self.__positionsList[i].keys() and not self.__positionsList[i - 1].keys():
                    ret['end'].append(self.__dateTimeList[i])
                    ret['duration'].append(ret['end'][-1] - ret['start'][-1])
            else:
                if not self.__positionsList[i].keys() and self.__positionsList[i - 1].keys():
                    ret['start'].append(self.__dateTimeList[i])
                elif self.__positionsList[i].keys() and not self.__positionsList[i - 1].keys():
                    ret['end'].append(self.__dateTimeList[i])
                    ret['duration'].append(ret['end'][-1] - ret['start'][-1])
        return ret

    def getAlpha(self):
        riskFreeRate = self.__strat.getRiskFreeRate()
        stratReturn = self.getAnnualizedReturn()
        numTradeDays = 250
        benchmarkReturn = self.getBenchmarkCumuReturns()[-1] * len(self.getBenchmarkCumuReturns()) / numTradeDays
        expectedReturn = riskFreeRate + self.getBeta() * (benchmarkReturn - riskFreeRate)
        alpha = stratReturn - expectedReturn
        return alpha

    def getBeta(self):
        Y = pd.Series(self.getReturns()[0:])
        X = sm.add_constant(self.getBenchmarkDailyReturns()[0:])
        model = sm.OLS(Y, X)
        results = model.fit()
        beta = results.params[1]
        return beta

    def getSortinoRatio(self):
        riskFreeRate = self.__strat.getRiskFreeRate()
        stratReturns = pd.Series(self.getReturns()[0:])
        numTradeDays = 250
        dailyRiskFreeRate = riskFreeRate / numTradeDays
        downsideReturns = pd.Series(
            [downsideReturn for downsideReturn in stratReturns if downsideReturn < dailyRiskFreeRate])
        downsideVolatility = downsideReturns.std()
        annualizedAvgReturn = stratReturns.mean() * numTradeDays / len(stratReturns)
        sortinoRatio = (annualizedAvgReturn - riskFreeRate) / downsideVolatility
        return sortinoRatio

    def getInformationRatio(self):
        stratReturns = pd.Series(self.getReturns()[0:])
        benchmarkReturns = pd.Series(self.getBenchmarkDailyReturns()[0:])
        residualReturns = stratReturns - self.getBeta() * benchmarkReturns
        residualReturnMean = residualReturns.mean()
        residualRisk = residualReturns.std()
        IR = residualReturnMean / residualRisk
        return IR

    def getEquitySeries(self):
        dateIndex = self.getDateTimeList()
        equityList = self.getEquityList()
        equitySeries = pd.Series(data=equityList, index=dateIndex)
        return equitySeries

    def getAnnualizedVolatility(self):
        """年化波动率"""
        returns = self.getEquitySeries().pct_change()
        dayVolatility = returns.std()
        numTradeDays = 250
        annualizedVolatility = numTradeDays ** 0.5 * dayVolatility
        return annualizedVolatility

    def getBenchmarkDailyReturns(self):
        return self.__benchmarkDailyReturns

    def getBenchmarkList(self):
        return self.__benchmarkList

    def getBenchmarkCumuReturns(self):
        cumuReturns = dataseries.SequenceDataSeries(maxLen=None)
        dateList = self.getDateTimeList()
        benchmarkList = self.getBenchmarkList()
        assert len(dateList) == len(benchmarkList)
        for i in range(0, len(benchmarkList)):
            cumuReturns.appendWithDateTime(dateTime=dateList[i],
                                           value=(benchmarkList[i] - benchmarkList[0]) / benchmarkList[0])
        return cumuReturns

    def generateReport(self, figname):
        """生成策略回测报告"""
        startx = 0
        starty = 0
        instruments = self.__strat.getFeed().getKeys()
        reportinstr = instruments[0]
        for i in range(0, len(instruments) - 1):
            if len(instruments) > 1:
                reportinstr = reportinstr + ',' + instruments[i + 1]
        name = reportinstr + u'策略回测绩效报告'
        self.__report = excelreport.ExcelReport(name)
        sheet1 = u"策略分析"
        sheet2 = u"交易列表"
        sheet3 = u"交易分析"
        sheet4 = u"周期分析"
        sheet5 = u'设置'
        self.__report.addSheet(sheet1)
        self.__report.addSheet(sheet2)
        self.__report.addSheet(sheet3)
        self.__report.addSheet(sheet4)
        self.__report.addSheet(sheet5)
        self.__report.write(sheet1, startx, starty, u'策略绩效概要', 20)
        sheet1ItemsList = [u'初始资本',
                           u'滑价',
                           u'净利润',
                           u'已付滑价',
                           u'已付手续费',
                           u'年化收益率',
                           u'夏普比率',
                           u'索提诺比率',
                           u'Alpha',
                           u'Beta',
                           u'信息比率'
                           u'交易周期',
                           u'策略运行时间',
                           u'最长空仓期',
                           u'最长回撤周期',
                           u'最大回撤',
                           u'最大回撤日期',
                           u'权益曲线']
        sheet1ValuesList = [self.__strat.getInitialCaptial(),
                            self.__strat.getBroker().getFillStrategy().getSlippageModel().getSlippagePerShare(),
                            self.getEquityList()[-1] - self.getEquityList()[0],
                            self.getPaidSlippage(),
                            self.getCommissionsForAllTrades().sum(),
                            self.getAnnualizedReturn(),
                            self.getSharpeRatio(riskFreeRate=self.__strat.getRiskFreeRate(), annualized=True),
                            self.getSortinoRatio(),
                            self.getAlpha(),
                            self.getBeta(),
                            self.getInformationRatio(),
                            self.getTradePeriod().days,
                            self.getStratRunTime().days,
                            self.getLongestEmptyDuration().days,
                            self.getLongestDrawDownDuration().days,
                            self.getMaxDrawDown(),
                            self.getMaxDrawDownDateTime().strftime('%Y-%m-%d')]
        self.__report.writeList(sheet1, startx, starty + 1, sheet1ItemsList)
        self.__report.writeList(sheet1, startx + 1, starty + 1, sheet1ValuesList)
        figname = figname
        self.__report.insertPicture(sheet1, startx + 2 + len(sheet1ValuesList), starty, figname)
        keysTradesListDict = [u'date', u'instrument', u'type', u'shares', u'price', u'cost', u'return', u'returnrate',
                              u'position']
        self.__report.writeListDict(sheet2, 0, 0, self.getTradesListDict(), 1, keys=keysTradesListDict)
        sheet3ItemsList = [u'交易总次数',
                           u'盈利交易次数',
                           u'亏损交易次数',
                           u'胜率',
                           u'平均盈利额',
                           u'平均亏损额',
                           u'平均盈利/平均亏损',
                           u'单笔最大盈利',
                           u'单笔最大亏损']
        sheet3ValuesList = [self.getCount(),
                            self.getProfitableCount(),
                            self.getUnprofitableCount(),
                            self.getWinningRatio(),
                            self.getAverageProfit(),
                            abs(self.getAverageLoss()),
                            self.getAvarageProfitLossRatio(),
                            self.getMaxProfit(),
                            abs(self.getMaxLosses())]
        self.__report.writeList(sheet3, startx, starty + 1, sheet3ItemsList)
        self.__report.writeList(sheet3, startx + 1, starty + 1, sheet3ValuesList)
        keysDailyPeriodAnalysis = [u'Date', u'profit', u'profitratio']
        self.__report.writeListDict(sheet4, 0, 0, self.getDailyPeriodAnalysis(), 1, keys=keysDailyPeriodAnalysis)
        keysByMonth = [u'Date', u'1 Month', u'3 Months', u'6 Months', u'12 Months']
        sheet6 = u'收益波动率'
        self.__report.addSheet(sheet6)
        self.__report.writeListDict(sheet6, 0, 0, self.getVolatilityDict(), 1, keys=keysByMonth)
        sheet7 = u'夏普比率'
        self.__report.addSheet(sheet7)
        self.__report.writeListDict(sheet7, 0, 0, self.getSharpeRatioDict(), 1, keys=keysByMonth)
        sheet8 = u'最大回撤'
        self.__report.addSheet(sheet8)
        self.__report.writeListDict(sheet8, 0, 0, self.getMaxDrawDownDict(), 1, keys=keysByMonth)
        sheet9 = u'Beta'
        self.__report.addSheet(sheet9)
        self.__report.writeListDict(sheet9, 0, 0, self.getBetaDict(), 1, keys=keysByMonth)
        sheet10 = u'信息比率'
        self.__report.addSheet(sheet10)
        self.__report.writeListDict(sheet10, 0, 0, self.getIRDict(), 1, keys=keysByMonth)
        sheet11 = u'Alpha'
        self.__report.addSheet(sheet11)
        self.__report.writeListDict(sheet11, 0, 0, self.getAlphaDict(), 1, keys=keysByMonth)
        self.__report.save()
        print
        "report done"

    def __countMonth(self):
        dateList = self.getDateTimeList()
        result = []
        for i in range(0, len(dateList)):
            dict = {}
            year = dateList[i].year
            month = dateList[i].month
            if i == 0:
                result.append([dateList[i]])
            elif dateList[i].month != dateList[i - 1].month and i != len(dateList) - 1:
                result[-1].append(dateList[i - 1])
                result.append([dateList[i]])
            elif i == len(dateList) - 1:
                if dateList[i].month != dateList[i - 1].month:
                    result[-1].append(dateList[i - 1])
                    result.append([dateList[i], dateList[i]])
                else:
                    result[-1].append(dateList[i])
        return result

    def getVolatilityDict(self):
        volatilityDict = {u'1 Month': [],
                          u'3 Months': [],
                          u'6 Months': [],
                          u'12 Months': []
                          }
        startEndList = self.__countMonth()
        returns = self.getReturns()[0:]
        dateList = self.getDateTimeList()
        volatilityDict[u'Date'] = [daylist[0].strftime('%Y-%m') for daylist in startEndList]
        for i in range(0, len(volatilityDict['Date'])):
            volatilityDict[u'1 Month'].append(self.__calculateVolatilityByMonth(i, 1, dateList, startEndList, returns))
            volatilityDict[u'3 Months'].append(self.__calculateVolatilityByMonth(i, 3, dateList, startEndList, returns))
            volatilityDict[u'6 Months'].append(self.__calculateVolatilityByMonth(i, 6, dateList, startEndList, returns))
            volatilityDict[u'12 Months'].append(
                self.__calculateVolatilityByMonth(i, 12, dateList, startEndList, returns))
        return volatilityDict

    def getSharpeRatioDict(self):
        riskFreeRate = self.__strat.getRiskFreeRate()
        sharpeRatioDict = {u'1 Month': [],
                           u'3 Months': [],
                           u'6 Months': [],
                           u'12 Months': []
                           }
        startEndList = self.__countMonth()
        returns = self.getReturns()[0:]
        dateList = self.getDateTimeList()
        sharpeRatioDict[u'Date'] = [daylist[0].strftime('%Y-%m') for daylist in startEndList]
        for i in range(0, len(sharpeRatioDict[u'Date'])):
            sharpeRatioDict[u'1 Month'].append(
                self.__calculateSharpeByMonth(i, 1, dateList, startEndList, returns, riskFreeRate))
            sharpeRatioDict[u'3 Months'].append(
                self.__calculateSharpeByMonth(i, 3, dateList, startEndList, returns, riskFreeRate))
            sharpeRatioDict[u'6 Months'].append(
                self.__calculateSharpeByMonth(i, 6, dateList, startEndList, returns, riskFreeRate))
            sharpeRatioDict[u'12 Months'].append(
                self.__calculateSharpeByMonth(i, 12, dateList, startEndList, returns, riskFreeRate))
        return sharpeRatioDict

    def getMaxDrawDownDict(self):
        maxDrawDownDict = {u'1 Month': [],
                           u'3 Months': [],
                           u'6 Months': [],
                           u'12 Months': []
                           }
        startEndList = self.__countMonth()
        returns = self.getReturns()[0:]
        dateList = self.getDateTimeList()
        equityList = self.getEquitySeries()
        maxDrawDownDict[u'Date'] = [daylist[0].strftime('%Y-%m') for daylist in startEndList]
        for i in range(0, len(maxDrawDownDict[u'Date'])):
            maxDrawDownDict[u'1 Month'].append(self.__calculateMaxDrawDown(i, 1, dateList, startEndList, equityList))
            maxDrawDownDict[u'3 Months'].append(self.__calculateMaxDrawDown(i, 3, dateList, startEndList, equityList))
            maxDrawDownDict[u'6 Months'].append(self.__calculateMaxDrawDown(i, 6, dateList, startEndList, equityList))
            maxDrawDownDict[u'12 Months'].append(self.__calculateMaxDrawDown(i, 12, dateList, startEndList, equityList))
        return maxDrawDownDict

    def getBetaDict(self):
        betaDict = {u'1 Month': [],
                    u'3 Months': [],
                    u'6 Months': [],
                    u'12 Months': []
                    }
        startEndList = self.__countMonth()
        returns = self.getReturns()[0:]
        benchmarkReturns = self.getBenchmarkDailyReturns()[0:]
        dateList = self.getDateTimeList()
        betaDict[u'Date'] = [daylist[0].strftime('%Y-%m') for daylist in startEndList]
        for i in range(0, len(betaDict[u'Date'])):
            betaDict[u'1 Month'].append(self.__caculateBeta(i, 1, dateList, startEndList, returns, benchmarkReturns))
            betaDict[u'3 Months'].append(self.__caculateBeta(i, 3, dateList, startEndList, returns, benchmarkReturns))
            betaDict[u'6 Months'].append(self.__caculateBeta(i, 6, dateList, startEndList, returns, benchmarkReturns))
            betaDict[u'12 Months'].append(self.__caculateBeta(i, 12, dateList, startEndList, returns, benchmarkReturns))
        return betaDict

    def getIRDict(self):
        IRDict = {u'1 Month': [],
                  u'3 Months': [],
                  u'6 Months': [],
                  u'12 Months': []}
        startEndList = self.__countMonth()
        dateList = self.getDateTimeList()
        IRDict[u'Date'] = [daylist[0].strftime('%Y-%m') for daylist in startEndList]
        stratReturns = pd.Series(self.getReturns()[0:])
        benchmarkReturns = pd.Series(self.getBenchmarkDailyReturns()[0:])
        for i in range(0, len(IRDict[u'Date'])):
            IRDict[u'1 Month'].append(self.__calculateIR(i, 1, dateList, startEndList, stratReturns, benchmarkReturns))
            IRDict[u'3 Months'].append(self.__calculateIR(i, 3, dateList, startEndList, stratReturns, benchmarkReturns))
            IRDict[u'6 Months'].append(self.__calculateIR(i, 6, dateList, startEndList, stratReturns, benchmarkReturns))
            IRDict[u'12 Months'].append(
                self.__calculateIR(i, 12, dateList, startEndList, stratReturns, benchmarkReturns))
        return IRDict

    def getAlphaDict(self):
        riskFreeRate = self.__strat.getRiskFreeRate()
        alphaDict = {u'1 Month': [],
                     u'3 Months': [],
                     u'6 Months': [],
                     u'12 Months': []}
        startEndList = self.__countMonth()
        dateList = self.getDateTimeList()
        alphaDict[u'Date'] = [daylist[0].strftime('%Y-%m') for daylist in startEndList]
        stratReturns = pd.Series(self.getReturns()[0:])
        benchmarkReturns = pd.Series(self.getBenchmarkDailyReturns()[0:])
        for i in range(0, len(alphaDict[u'Date'])):
            alphaDict[u'1 Month'].append(
                self.__calculateAlpha(i, 1, dateList, startEndList, stratReturns, benchmarkReturns, riskFreeRate))
            alphaDict[u'3 Months'].append(
                self.__calculateAlpha(i, 3, dateList, startEndList, stratReturns, benchmarkReturns, riskFreeRate))
            alphaDict[u'6 Months'].append(
                self.__calculateAlpha(i, 6, dateList, startEndList, stratReturns, benchmarkReturns, riskFreeRate))
            alphaDict[u'12 Months'].append(
                self.__calculateAlpha(i, 12, dateList, startEndList, stratReturns, benchmarkReturns, riskFreeRate))
        return alphaDict

    def __calculateAlpha(self, monthIndex, n, dateList, startEndList, returns, benchmarkReturns, riskFreeRate):
        if monthIndex < n - 1:
            return 'NaN'
        else:
            endDay = startEndList[monthIndex][1]
            endIndex = dateList.index(endDay)
            endStratReturn = returns[endIndex]
            endBenchmarkReturn = benchmarkReturns[endIndex]
            annualizedReturn = endStratReturn * 12 / n
            annualizedBenchmarkReturn = endBenchmarkReturn * 12 / n
            if n == 1:
                beta = self.getBetaDict()[u'1 Month'][monthIndex]
            elif n == 3:
                beta = self.getBetaDict()[u'3 Months'][monthIndex]
            elif n == 6:
                beta = self.getBetaDict()[u'6 Months'][monthIndex]
            elif n == 12:
                beta = self.getBetaDict()[u'12 Months'][monthIndex]
            alpha = (annualizedReturn - riskFreeRate) - beta * (annualizedBenchmarkReturn - riskFreeRate)
            return alpha

    def __calculateIR(self, monthIndex, n, dateList, startEndList, returns, benchmarkReturns):
        if monthIndex < n - 1:
            return 'NaN'
        else:
            startDay = startEndList[monthIndex - n + 1][0]
            endDay = startEndList[monthIndex][1]
            startIndex = dateList.index(startDay)
            endIndex = dateList.index(endDay)
            stratReturnsBetween = returns[startIndex:endIndex]
            benchamrkReturnsBetweem = benchmarkReturns[startIndex:endIndex]
            if n == 1:
                beta = self.getBetaDict()[u'1 Month'][monthIndex]
            elif n == 3:
                beta = self.getBetaDict()[u'3 Months'][monthIndex]
            elif n == 6:
                beta = self.getBetaDict()[u'6 Months'][monthIndex]
            elif n == 12:
                beta = self.getBetaDict()[u'12 Months'][monthIndex]
            residualReturns = stratReturnsBetween - beta * benchamrkReturnsBetweem
            residualReturnMean = residualReturns.mean()
            residualRisk = residualReturns.std()
            IR = residualReturnMean / residualRisk
            return IR

    def __caculateBeta(self, monthIndex, n, dateList, startEndList, returns, benchmarkReturns):
        if monthIndex < n - 1:
            return 'NaN'
        else:
            startDay = startEndList[monthIndex - n + 1][0]
            endDay = startEndList[monthIndex][1]
            startIndex = dateList.index(startDay)
            endIndex = dateList.index(endDay)
            returnsBetween = returns[startIndex:endIndex]
            benchamrkReturnsBetweem = benchmarkReturns[startIndex:endIndex]
            Y = pd.Series(returnsBetween)
            X = sm.add_constant(benchamrkReturnsBetweem)
            model = sm.OLS(Y, X)
            results = model.fit()
            beta = results.params[1]
            return beta

    def __calculateMaxDrawDown(self, monthIndex, n, dateList, startEndList, equityList):
        if monthIndex < n - 1:
            return 'NaN'
        else:
            startDay = startEndList[monthIndex - n + 1][0]
            endDay = startEndList[monthIndex][1]
            startIndex = dateList.index(startDay)
            endIndex = dateList.index(endDay)
            equityBetween = equityList[startIndex:endIndex]
            maxDrawDown = 0.0
            for i in range(0, len(equityBetween)):
                for j in range(i, len(equityBetween)):
                    drawdown = (equityBetween[j] - equityBetween[i]) / equityBetween[i]
                    maxDrawDown = max(maxDrawDown, -drawdown)
                    maxDrawDownDayIndex = j
            maxDrawDownDay = dateList[startIndex:endIndex][maxDrawDownDayIndex].strftime('%Y-%m-%d')
            maxDDPlusDay = str(maxDrawDown) + '(' + maxDrawDownDay + ')'
            return maxDDPlusDay

    def __calculateVolatilityByMonth(self, monthIndex, n, dateList, startEndList, returns):
        if monthIndex < n - 1:
            return 'NaN'
        else:
            startDay = startEndList[monthIndex - n + 1][0]
            endDay = startEndList[monthIndex][1]
            startIndex = dateList.index(startDay)
            endIndex = dateList.index(endDay)
            returnsBetween = pd.Series(returns[startIndex:endIndex])
            volatility = returnsBetween.std()
            return volatility * 250 ** 0.5

    def __calculateSharpeByMonth(self, monthIndex, n, dateList, startEndList, returns, rf):
        if monthIndex < n - 1:
            return 'NaN'
        else:
            startDay = startEndList[monthIndex - n + 1][0]
            endDay = startEndList[monthIndex][1]
            startIndex = dateList.index(startDay)
            endIndex = dateList.index(endDay)
            returnsBetween = pd.Series(returns[startIndex:endIndex])
            annualizedVolatility = returnsBetween.std() * 250 ** 0.5
            annualizedReturn = returnsBetween.mean() * 12 / n
            sharpe = (annualizedReturn - rf) / annualizedVolatility
            return sharpe
