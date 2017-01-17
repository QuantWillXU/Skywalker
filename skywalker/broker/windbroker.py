# -*- coding:utf-8 -*-
import pandas as pd
from WindPy import *

from skywalker import broker


class WindBroker(broker.Broker):
    def __init__(self, barFeed=None, DepartmentID=0,
                 BrokerID=u'0000', LogonAccount=u'w2617001001', AccountType=u'SHSZ', Password=u'111'):
        super(WindBroker, self).__init__()
        self.__logOnWindBroker(DepartmentID=DepartmentID,
                               BrokerID=BrokerID, LogonAccount=LogonAccount, AccountType=AccountType, Password=Password)
        self.__barFeed = barFeed
        self.__shares = None
        self.__cash = None
        self.__equity = None
        self.__activeOrders = {}

    def __logOnWindBroker(self, DepartmentID, BrokerID, LogonAccount, AccountType, Password):
        w.start()
        w.tlogon(DepartmentID=DepartmentID,
                 BrokerID=BrokerID, LogonAccount=LogonAccount, AccountType=AccountType, Password=Password)

    def getOrdersFromWind(self):
        errorCode = 1
        while errorCode != 0:
            orders = w.tquery(qrycode=2)
            errorCode = orders.ErrorCode
        orderDF = pd.DataFrame(data=orders.Data)
        orderDF = orderDF.T
        orderDF.columns = orders.Fields
        return orderDF

    def getPositions(self):
        self.refreshPostion()
        return self.__shares

    def getCash(self):
        self.refreshAsset()
        return self.__cash

    def getEquity(self):
        self.refreshAsset()
        return self.__equity

    def refreshActiveOrders(self):
        orderDF = self.getOrdersFromWind()
        try:
            orderDF = orderDF[orderDF['Remark'] == u'已报']
            orderList = [self.__series2Order(orderDF.iloc[i]) for i in range(len(orderDF))]
            activeorders = {ID: order for ID, order in zip([order.getId() for order in orderList], orderList)}
        except KeyError:
            activeorders = {}
        self.__activeOrders = activeorders

    def __series2Order(self, series):
        action = series['TradeSide']
        if action == 'Buy':
            action = broker.Order.Action.BUY
        elif action == 'Sell':
            action = broker.Order.Action.SELL
        instrument = series['SecurityCode']
        quantity = series['OrderVolume']
        order = broker.Order(type_=broker.Order.Type.NEXT_CUSTOM_TYPE,
                             action=action, instrument=instrument, quantity=quantity,
                             instrumentTraits=self.getInstrumentTraits(instrument))
        order.setSubmitted(orderId=series['OrderNumber'], dateTime=series['OrderTime'])
        order.setState(newState=broker.Order.State.SUBMITTED)
        return order

    def refreshPostion(self):
        positions = w.tquery(qrycode=1)
        posDF = pd.DataFrame(positions.Data)
        posDF = posDF.T
        posDF.columns = positions.Fields
        refreshedPos = {k: v for k, v in zip(posDF['SecurityCode'], posDF['SecurityVolume'])}
        self.__shares = refreshedPos
        # return self.__shares

    def refreshAsset(self):
        cash = w.tquery(qrycode=0)
        cashDF = pd.DataFrame(cash.Data)
        cashDF = cashDF.T
        cashDF.columns = cash.Fields
        self.__cash = cashDF['AvailableFund'][0]
        self.__equity = cashDF['TotalAsset'][0]

    def eof(self):
        return self.__barFeed.eof()

    def peekDateTime(self):
        return None

    def getCash(self, includeShort=True):
        self.refreshAsset()
        return self.__cash

    def getInstrumentTraits(self, instrument):
        return broker.IntegerTraits()

    def getShares(self, instrument):
        self.refreshPostion()
        return self.__shares.get(instrument, 0)

    def getPositions(self):
        self.refreshPostion()
        return self.__shares

    def getActiveOrders(self, instrument=None):
        self.refreshActiveOrders()
        return self.__activeOrders.values()

    def submitOrder(self, order):
        OrderType = order.getType()
        if OrderType == 2:
            OrderType = 0
        elif OrderType == 1:
            OrderType = 4
        SecurityCode = order.getInstrument()
        OrderVolume = order.getQuantity()
        TradeSide = order.getAction()
        if TradeSide == 1:
            pass
        elif TradeSide == 3:
            TradeSide = 4
        if isinstance(order, broker.LimitOrder):
            OrderPrice = order.getLimitPrice()
        else:
            OrderPrice = 0
        windOrder = w.torder(SecurityCode=SecurityCode, TradeSide=TradeSide, OrderType=OrderType,
                             OrderVolume=OrderVolume, OrderPrice=OrderPrice)
        pass

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        if onClose is True and self.__barFeed.isIntraday():
            raise Exception("Market-on-close not supported with intraday feeds")

        return broker.MarketOrder(action, instrument, quantity, onClose, self.getInstrumentTraits(instrument))

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return broker.LimitOrder(action, instrument, limitPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("Not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("Not supported")

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")
        w.tcancel(order.getId())
        self.refreshActiveOrders()

    def dispatch(self):
        self.refreshAsset()
        self.refreshPostion()
        self.refreshActiveOrders()

    def join(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass


if __name__ == '__main__':
    brk = WindBroker()
    # order = brk.createLimitOrder(action=1, instrument='300345.SZ', limitPrice=10, quantity=200)
    # brk.submitOrder(order)
    ActiveOrders = brk.refreshActiveOrders()
    # order = ActiveOrders[ActiveOrders.keys()[0]]
    # brk.cancelOrder(order)
    # print brk.getOrdersFromWind()
