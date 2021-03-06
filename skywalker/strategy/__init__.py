# -*- coding:utf-8 -*-

# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import abc
import logging

import skywalker.broker
import skywalker.strategy.position
from skywalker import dataseries
from skywalker import dispatcher
from skywalker import logger
from skywalker import observer
from skywalker import skywalkerplotter
from skywalker.barfeed import pgfeed_nowind
from skywalker.barfeed import resampled
from skywalker.broker import backtesting
from skywalker.performance.do_cprofile import do_cprofile
from skywalker.stratanalyzer import analyzer_skywalker


class BaseStrategy(object):
    """Base class for strategies.

    :param barFeed: The bar feed that will supply the bars.
    :type barFeed: :class:`pyalgotrade.barfeed.BaseBarFeed`.
    :param broker: The broker that will handle orders.
    :type broker: :class:`pyalgotrade.broker.Broker`.

    .. note::
        This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    LOGGER_NAME = "strategy"

    def __init__(self, barFeed, broker):
        self.__barFeed = barFeed
        self.__broker = broker
        self.__activePositions = set()
        self.__orderToPosition = {}
        self.__barsProcessedEvent = observer.Event()
        self.__analyzers = []
        self.__namedAnalyzers = {}
        self.__resampledBarFeeds = []
        self.__dispatcher = dispatcher.Dispatcher()
        self.__broker.getOrderUpdatedEvent().subscribe(self.__onOrderEvent)
        self.__barFeed.getNewValuesEvent().subscribe(self.__onBars)

        self.__dispatcher.getStartEvent().subscribe(self.onStart)
        self.__dispatcher.getIdleEvent().subscribe(self.__onIdle)

        # It is important to dispatch broker events before feed events, specially if we're backtesting.
        self.__dispatcher.addSubject(self.__broker)
        self.__dispatcher.addSubject(self.__barFeed)

        # Initialize logging.
        self.__logger = logger.getLogger(BaseStrategy.LOGGER_NAME)
        self.__count = 0

    # Only valid for testing purposes.
    def _setBroker(self, broker):
        self.__broker = broker

    def setUseEventDateTimeInLogs(self, useEventDateTime):
        if useEventDateTime:
            logger.Formatter.DATETIME_HOOK = self.getDispatcher().getCurrentDateTime
        else:
            logger.Formatter.DATETIME_HOOK = None

    def getLogger(self):
        return self.__logger

    def getActivePositions(self):
        return self.__activePositions

    def getOrderToPosition(self):
        return self.__orderToPosition

    def getDispatcher(self):
        return self.__dispatcher

    def getResult(self):
        return self.getBroker().getEquity()

    def getBarsProcessedEvent(self):
        return self.__barsProcessedEvent

    def getUseAdjustedValues(self):
        return False

    def registerPositionOrder(self, position, order):
        self.__activePositions.add(position)
        assert (order.isActive())  # Why register an inactive order ?
        self.__orderToPosition[order.getId()] = position

    def unregisterPositionOrder(self, position, order):
        del self.__orderToPosition[order.getId()]

    def unregisterPosition(self, position):
        assert (not position.isOpen())
        self.__activePositions.remove(position)

    def __notifyAnalyzers(self, lambdaExpression):
        for s in self.__analyzers:
            lambdaExpression(s)

    def attachAnalyzerEx(self, strategyAnalyzer, name=None):
        if strategyAnalyzer not in self.__analyzers:
            if name is not None:
                if name in self.__namedAnalyzers:
                    raise Exception("A different analyzer named '%s' was already attached" % name)
                self.__namedAnalyzers[name] = strategyAnalyzer

            strategyAnalyzer.beforeAttach(self)
            self.__analyzers.append(strategyAnalyzer)
            strategyAnalyzer.attached(self)

    def getLastPrice(self, instrument):
        ret = None
        bar = self.getFeed().getLastBar(instrument)
        if bar is not None:
            ret = bar.getPrice()
        return ret

    def getFeed(self):
        """Returns the :class:`pyalgotrade.barfeed.BaseBarFeed` that this strategy is using."""
        return self.__barFeed

    def getBroker(self):
        """Returns the :class:`pyalgotrade.broker.Broker` used to handle order executions."""
        return self.__broker

    def getCurrentDateTime(self):
        """Returns the :class:`datetime.datetime` for the current :class:`pyalgotrade.bar.Bars`."""
        return self.__barFeed.getCurrentDateTime()

    def marketOrder(self, instrument, quantity, onClose=False, goodTillCanceled=False, allOrNone=False):
        """Submits a market order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param onClose: True if the order should be filled as close to the closing price as possible (Market-On-Close order). Default is False.
        :type onClose: boolean.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.MarketOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createMarketOrder(skywalker.broker.Order.Action.BUY, instrument, quantity,
                                                     onClose)
        elif quantity < 0:
            ret = self.getBroker().createMarketOrder(skywalker.broker.Order.Action.SELL, instrument,
                                                     quantity * -1, onClose)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def limitOrder(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a limit order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.LimitOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createLimitOrder(skywalker.broker.Order.Action.BUY, instrument,
                                                    limitPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createLimitOrder(skywalker.broker.Order.Action.SELL, instrument,
                                                    limitPrice, quantity * -1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def stopOrder(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a stop order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.StopOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createStopOrder(skywalker.broker.Order.Action.BUY, instrument, stopPrice,
                                                   quantity)
        elif quantity < 0:
            ret = self.getBroker().createStopOrder(skywalker.broker.Order.Action.SELL, instrument, stopPrice,
                                                   quantity * -1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def stopLimitOrder(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a stop limit order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.StopLimitOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createStopLimitOrder(skywalker.broker.Order.Action.BUY, instrument,
                                                        stopPrice, limitPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createStopLimitOrder(skywalker.broker.Order.Action.SELL, instrument,
                                                        stopPrice, limitPrice, quantity * -1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def enterLong(self, instrument, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.MarketOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """
        return skywalker.strategy.position.LongPosition(self, instrument, None, None, quantity,
                                                        goodTillCanceled, allOrNone)

    def enterShort(self, instrument, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.MarketOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.ShortPosition(self, instrument, None, None, quantity,
                                                         goodTillCanceled, allOrNone)

    def enterLongLimit(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.LimitOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.LongPosition(self, instrument, None, limitPrice, quantity,
                                                        goodTillCanceled, allOrNone)

    def enterShortLimit(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.LimitOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.ShortPosition(self, instrument, None, limitPrice, quantity,
                                                         goodTillCanceled, allOrNone)

    def enterLongStop(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.StopOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.LongPosition(self, instrument, stopPrice, None, quantity,
                                                        goodTillCanceled, allOrNone)

    def enterShortStop(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.StopOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.ShortPosition(self, instrument, stopPrice, None, quantity,
                                                         goodTillCanceled, allOrNone)

    def enterLongStopLimit(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.StopLimitOrder` order to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.LongPosition(self, instrument, stopPrice, limitPrice, quantity,
                                                        goodTillCanceled, allOrNone)

    def enterShortStopLimit(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.StopLimitOrder` order to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: The Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return skywalker.strategy.position.ShortPosition(self, instrument, stopPrice, limitPrice, quantity,
                                                         goodTillCanceled, allOrNone)

    def onEnterOk(self, position):
        """Override (optional) to get notified when the order submitted to enter a position was filled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    def onEnterCanceled(self, position):
        """Override (optional) to get notified when the order submitted to enter a position was canceled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    # Called when the exit order for a position was filled.
    def onExitOk(self, position):
        """Override (optional) to get notified when the order submitted to exit a position was filled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    # Called when the exit order for a position was canceled.
    def onExitCanceled(self, position):
        """Override (optional) to get notified when the order submitted to exit a position was canceled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    """Base class for strategies. """

    def onStart(self):
        """Override (optional) to get notified when the strategy starts executing. The default implementation is empty. """
        pass

    def onFinish(self, bars):
        """Override (optional) to get notified when the strategy finished executing. The default implementation is empty.

        :param bars: The last bars processed.
        :type bars: :class:`pyalgotrade.bar.Bars`.
        """
        pass

    def onIdle(self):
        """Override (optional) to get notified when there are no events.

       .. note::
            In a pure backtesting scenario this will not be called.
        """
        pass

    @abc.abstractmethod
    def onBars(self, bars):
        """Override (**mandatory**) to get notified when new bars are available. The default implementation raises an Exception.

        **This is the method to override to enter your trading logic and enter/exit positions**.

        :param bars: The current bars.
        :type bars: :class:`pyalgotrade.bar.Bars`.
        """
        raise NotImplementedError()

    def onOrderUpdated(self, order):
        """Override (optional) to get notified when an order gets updated.

        :param order: The order updated.
        :type order: :class:`pyalgotrade.broker.Order`.
        """
        pass

    def __onIdle(self):
        # Force a resample check to avoid depending solely on the underlying
        # barfeed events.
        for resampledBarFeed in self.__resampledBarFeeds:
            resampledBarFeed.checkNow(self.getCurrentDateTime())

        self.onIdle()

    def __onOrderEvent(self, broker_, orderEvent):
        order = orderEvent.getOrder()
        self.onOrderUpdated(order)

        # Notify the position about the order event.
        pos = self.__orderToPosition.get(order.getId(), None)
        if pos is not None:
            # Unlink the order from the position if its not active anymore.
            if not order.isActive():
                self.unregisterPositionOrder(pos, order)

            pos.onOrderEvent(orderEvent)

    def __fixPostionsAndCash(self, datetime):
        pos = self.getBroker().getPositions()
        for instrument in pos.keys():
            if self.getFeed().haveEquityEvent(instrument, datetime):
                equityEvent = self.getFeed().getEquityEvent(instrument)
                event = equityEvent[datetime:datetime]
                div_capitalization = event['div_capitalization'].iloc[0]
                div_stock = event['div_stock'].iloc[0]
                div_cashaftertax = event['div_cashaftertax'].iloc[0]
                self.getBroker().fixPositions(instrument, pos[instrument] * float(1 + div_capitalization + div_stock))
                self.getBroker().setCash(self.getBroker().getCash() + pos[instrument] * div_cashaftertax)

    def __onBars(self, dateTime, bars):
        # THE ORDER HERE IS VERY IMPORTANT
        #

        self.__fixPostionsAndCash(bars.getDateTime())
        # self.getBroker().setCash(bars['300345'].getOpen())



        # 1: Let analyzers process bars.
        self.__notifyAnalyzers(lambda s: s.beforeOnBars(self, bars))

        # 2: Let the strategy process current bars and submit orders.
        self.onBars(bars)

        # 3: Notify that the bars were processed.
        self.__barsProcessedEvent.emit(self, bars)

        # self.getBroker().fixPositions('600128.SH', 100000)
        #

    def run(self):
        """Call once (**and only once**) to run the strategy."""
        self.__dispatcher.run()

        if self.__barFeed.getCurrentBars() is not None:
            self.onFinish(self.__barFeed.getCurrentBars())
        else:
            raise Exception("Feed was empty")

    def stop(self):
        """Stops a running strategy."""
        self.__dispatcher.stop()

    def attachAnalyzer(self, strategyAnalyzer):
        """Adds a :class:`pyalgotrade.stratanalyzer.StrategyAnalyzer`."""
        self.attachAnalyzerEx(strategyAnalyzer)

    def getNamedAnalyzer(self, name):
        return self.__namedAnalyzers.get(name, None)

    def debug(self, msg):
        """Logs a message with level DEBUG on the strategy logger."""
        self.getLogger().debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the strategy logger."""
        self.getLogger().info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the strategy logger."""
        self.getLogger().warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the strategy logger."""
        self.getLogger().error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the strategy logger."""
        self.getLogger().critical(msg)

    def resampleBarFeed(self, frequency, callback):
        """
        Builds a resampled barfeed that groups bars by a certain frequency.

        :param frequency: The grouping frequency in seconds. Must be > 0.
        :param callback: A function similar to onBars that will be called when new bars are available.
        :rtype: :class:`pyalgotrade.barfeed.BaseBarFeed`.
        """
        ret = resampled.ResampledBarFeed(self.getFeed(), frequency)
        ret.getNewValuesEvent().subscribe(callback)
        self.getDispatcher().addSubject(ret)
        self.__resampledBarFeeds.append(ret)
        return ret


class BacktestingStrategy(BaseStrategy):
    """Base class for backtesting strategies.

    :param barFeed: The bar feed to use to backtest the strategy.
    :type barFeed: :class:`pyalgotrade.barfeed.BaseBarFeed`.
    :param cash_or_brk: The starting capital or a broker instance.
    :type cash_or_brk: int/float or :class:`pyalgotrade.broker.Broker`.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, barFeed, cash_or_brk=1000000):
        # The broker should subscribe to barFeed events before the strategy.
        # This is to avoid executing orders submitted in the current tick.

        if isinstance(cash_or_brk, skywalker.broker.Broker):
            broker = cash_or_brk
        else:
            broker = backtesting.Broker(cash_or_brk, barFeed)

        BaseStrategy.__init__(self, barFeed, broker)
        self.__useAdjustedValues = False
        self.setUseEventDateTimeInLogs(True)
        self.setDebugMode(True)

    def getUseAdjustedValues(self):
        return self.__useAdjustedValues

    def setUseAdjustedValues(self, useAdjusted):
        self.getFeed().setUseAdjustedValues(useAdjusted)
        self.getBroker().setUseAdjustedValues(useAdjusted)
        self.__useAdjustedValues = useAdjusted

    def setDebugMode(self, debugOn):
        """Enable/disable debug level messages in the strategy and backtesting broker.
        This is enabled by default."""
        level = logging.DEBUG if debugOn else logging.INFO
        self.getLogger().setLevel(level)
        self.getBroker().getLogger().setLevel(level)


class SkysenseBacktestingStrategy(BacktestingStrategy):
    def __init__(self, cash_or_brk=1000000, feed=pgfeed_nowind.Feed(), analyzer=analyzer_skywalker.analyzer_skysense()):
        super(SkysenseBacktestingStrategy, self).__init__(feed, cash_or_brk=cash_or_brk)
        self.skysenseAnalyzer = analyzer
        self.attachAnalyzer(self.skysenseAnalyzer)
        self.__initialCaptial = cash_or_brk
        self.__benchmark = None
        self.__universe = None
        self.__start = None
        self.__end = None
        self.__riskFreeRate = 0

    def getRiskFreeRate(self):
        return self.__riskFreeRate

    def setRiskFreeRate(self, riskFreeRate):
        self.__riskFreeRate = riskFreeRate

    def setAllowNegativeCash(self, allowNegativeCash):
        self.getBroker().setAllowNegativeCash(allowNegativeCash)

    def getCommission(self):
        commission = self.getBroker().getCommission()
        return commission

    def setCommission(self, commission):
        self.getBroker().setCommission(commission)

    def getActiveOrders(self, instrument=None):
        ret = self.getBroker().getActiveOrders(instrument=instrument)
        return ret

    def setCash(self, cash):
        self.getBroker().setCash(cash)

    def getActiveInstruments(self):
        ret = self.getBroker().getActiveInstruments()
        return ret

    def getUniverse(self):
        return self.__universe

    def getStartDate(self):
        return self.__start

    def getEndDate(self):
        return self.__end

    @do_cprofile("./strat_run.prof", )
    def run(self, generateReport=True, haveStrat=True, show=False):
        self.loadData()
        super(SkysenseBacktestingStrategy, self).run()
        plt = skywalkerplotter.StrategyPlotter(self)
        plt.plotReturns(haveStrat, show)
        if generateReport:
            self.skysenseAnalyzer.generateReport('returns.jpg')

    def loadData(self):
        all = self.__universe
        if self.__benchmark in all:
            all.remove(self.__benchmark)
        all.append(self.__benchmark)
        try:
            self.getFeed().loadBars(all, fromDateTime=self.__start, toDateTime=self.__end, extra=['trade_status'])
        except Exception:
            pass

    def setStartDate(self, start):
        self.__start = start

    def setEndDate(self, end):
        self.__end = end

    def setBenchmenk(self, benchmark):
        self.__benchmark = benchmark

    def setUniverse(self, universe=None):
        if universe is None:
            self.__universe = self.getFeed().getAllInstruments()
        else:
            self.__universe = universe

    def getBenchmark(self):
        return self.__benchmark

    def getInstruments(self):
        return self.__universe

    def getInitialCaptial(self):
        return self.__initialCaptial

    def getShares(self, instrument):
        return self.getBroker().getShares(instrument)

    def getPositions(self):
        return self.getBroker().getPositions()

    def getCash(self):
        """获取现金"""
        return self.getBroker().getCash()

    def getEquity(self):
        """获取权益"""
        return self.getBroker().getEquity()

    def getFundamental(self, instrument, field, returnSeries=False):
        ret = self.getFeed()[instrument].getExtraDataSeries(field)
        if returnSeries:
            return ret.getPandasSeries()
        else:
            return ret

    def getOpen(self, instrument, returnSeries=False):
        ret = self.getFeed()[instrument].getOpenDataSeries()
        if returnSeries:
            return ret.getPandasSeries()
        else:
            return ret

    def getClose(self, instrument, returnSeries=False):
        ret = self.getFeed()[instrument].getCloseDataSeries()
        if returnSeries:
            return ret.getPandasSeries()
        else:
            return ret

    def getHigh(self, instrument, returnSeries=False):
        ret = self.getFeed()[instrument].getHighDataSeries()
        if returnSeries:
            return ret.getPandasSeries()
        else:
            return ret

    def getLow(self, instrument, returnSeries=False):
        ret = self.getFeed()[instrument].getLowDataSeries()
        if returnSeries:
            return ret.getPandasSeries()
        else:
            return ret

    def getVolume(self, instrument, returnSeries=False):
        ret = self.getFeed()[instrument].getVolumeDataSeries()
        if returnSeries:
            return ret.getPandasSeries()
        else:
            return ret

    def getForwardAdjOpen(self, instrument, returnSeries=False):
        ret = self.getOpen(instrument, True) * self.getAdjFactor(instrument)
        if returnSeries:
            return ret
        else:
            return dataseries.series2DataSeries(ret, ret.index)

    def getForwardAdjClose(self, instrument, returnSeries=False):
        ret = self.getClose(instrument, True) * self.getAdjFactor(instrument)
        if returnSeries:
            return ret
        else:
            return dataseries.series2DataSeries(ret, ret.index)

    def getForwardAdjHigh(self, instrument, returnSeries=False):
        ret = self.getHigh(instrument, True) * self.getAdjFactor(instrument)
        if returnSeries:
            return ret
        else:
            return dataseries.series2DataSeries(ret, ret.index)

    def getForwardAdjLow(self, instrument, returnSeries=False):
        ret = self.getLow(instrument, True) * self.getAdjFactor(instrument)
        if returnSeries:
            return ret
        else:
            return dataseries.series2DataSeries(ret, ret.index)

    def getBackwardAdjClose(self, instrument):
        return self.getFeed()[instrument].getAdjCloseDataSeries().getPandasSeries()

    def getPrice(self, instrument):
        return self.getFeed()[instrument].getPriceDataSeries().getPandasSeries()

    def getTradeStatus(self, instrument):
        return self.getFeed()[instrument].getExtraDataSeries("trade_status").getPandasSeries()

    def getAdjFactor(self, instrument):
        ret = self.getBackwardAdjClose(instrument) / self.getClose(instrument)
        ret = ret / ret[-1]
        return ret
