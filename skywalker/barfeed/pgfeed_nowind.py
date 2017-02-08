# -*- coding:utf-8 -*-
# PyAlgoTrade_skysense

from datetime import datetime
from math import isnan

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from skywalker import bar
from skywalker.barfeed import dbfeed
from skywalker.barfeed import membf
from skywalker.utils import dt


def normalize_instrument(instrument):
    return instrument.upper()


class Database(dbfeed.Database):
    def __init__(self, host='localhost', port=5432, user='postgres', password='123456', database='Skywalker'):
        self.__instrumentIds = {}
        self.__connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
        self.__cursor = self.__connection.cursor()
        self.__engine = create_engine('postgresql://postgres:123456@localhost:5432/Skywalker')

    def __findInstrumentId(self, instrument):
        cursor = self.__connection.cursor()
        sql = "select instrument_id from instrument where name = %s"
        cursor.execute(sql, [instrument])
        self.__connection.commit()
        ret = cursor.fetchone()
        if ret is not None:
            ret = ret[0]
        cursor.close()
        return ret

    def connection(self):
        return self.__connection

    def __addInstrument(self, instrument):
        self.__cursor.execute("insert into instrument (name) values (%s)", [instrument])
        self.__connection.commit()
        return self.__findInstrumentId(instrument)

    def __getOrCreateInstrument(self, instrument):
        # Try to get the instrument id from the cache.
        ret = self.__instrumentIds.get(instrument, None)
        if ret is not None:
            return ret
        # If its not cached, get it from the db.
        ret = self.__findInstrumentId(instrument)
        # If its not in the db, add it.
        if ret is None:
            ret = self.__addInstrument(instrument)
        # Cache the id.
        self.__instrumentIds[instrument] = ret
        return ret

    def getAllInstruments(self):
        sql = 'select name from instrument'
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        self.__connection.commit()
        instruments = []
        for record in cursor:
            instruments.append(record[0])
        return instruments

    def getInstrumentData(self, id):
        sql = 'select * from bar where instrument_id={id}'
        cursor = self.__connection.cursor()
        cursor.execute(sql.format(id=id))
        self.__connection.commit()
        ret = []
        for record in cursor:
            ret.append(record)
        return ret

    def getAllInstruments(self):
        sql = 'select name from instrument'
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        self.__connection.commit()
        ret = cursor.fetchall()
        ret = [tup[0] for tup in ret]
        return ret

    def createEquityEventTab(self):
        sql = '''create table equityevent (
        instrument_id integer references instrument (instrument_id),
        div_exdate integer not null,
        div_capitalization real not null,
        div_stock real not null,
        div_cashaftertax real not null,
        primary key (instrument_id, div_exdate))
        '''
        self.__cursor.execute(sql)
        self.__connection.commit()

    def getEquityEvent(self, instrument):
        sql = """select instrument.name,equityevent.div_exdate,equityevent.div_capitalization,equityevent.div_stock,equityevent.div_cashaftertax
        from equityevent join instrument on (equityevent.instrument_id = instrument.instrument_id)
        where instrument.name = %s
        """
        cursor = self.__connection.cursor()
        cursor.execute(sql, [instrument])
        self.__connection.commit()
        name = []
        div_exdate = []
        div_capitalization = []
        div_stock = []
        div_cashaftertax = []
        for row in cursor:
            name.append(row[0])
            div_exdate.append(dt.timestamp_to_datetime(row[1]))
            div_capitalization.append(row[2])
            div_stock.append(row[3])
            div_cashaftertax.append(row[4])
        keys = ['instrument', 'div_exdate', 'div_capitalization', 'div_stock', 'div_cashaftertax']
        ret = pd.DataFrame(
            {k: v for k, v in zip(keys, [name, div_exdate, div_capitalization, div_stock, div_cashaftertax])})
        ret = ret.set_index('div_exdate')
        return ret

    def createSchema(self):
        sqlCreateInst = '''create table instrument (
        instrument_id SERIAL primary key ,
        name text unique not null)'''
        self.__cursor.execute(sqlCreateInst)
        self.__connection.commit()
        self.__cursor.execute(
            "create table bar ("
            "instrument_id integer references instrument (instrument_id)"
            ", frequency integer not null"
            ", timestamp integer not null"
            ", open real not null"
            ", high real not null"
            ", low real not null"
            ", close real not null"
            ", volume real not null"
            ", adj_close real"
            ", trade_status integer"
            ", primary key (instrument_id, frequency, timestamp))")
        self.__connection.commit()

    def createFundamentalTab(self):
        sqlCreate = """create table fundamental (
        instrument_id integer references instrument (instrument_id),
        field text not null,
        frequency integer not null,
        timestamp integer not null,
        primary key (instrument_id, field, frequency, timestamp))"""
        self.__cursor.execute(sqlCreate)
        self.__connection.commit()

    def getFundamental(self, instrument, field, fromdate, todate=None):
        pass

    def getFundamentalFields(self):
        """ 返回表格的字段"""
        sql = '''SELECT a.attname as name
        FROM pg_class as c,pg_attribute as a
        where c.relname = 'fundamental' and a.attrelid = c.oid and a.attnum>0  '''
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        fields = cursor.fetchall()
        fields = [tup[0] for tup in fields]
        # fields = list(zip(*fields)[0])
        # 删除某个字段后postgreSQL数据库中会留下类似........pg.dropped.9........的字段，需要将其去除
        fields = [i for i in fields if '........' not in i]
        return fields


    def importBarsFromCSV(self, filename, instrument):
        data = pd.read_csv(filename, encoding='gbk')
        for k in range(0, len(data)):
            extrabar = {}
            if data.iloc[k]['TRADE_STATUS'] == u'交易':
                extrabar['trade_status'] = 1
            else:
                extrabar['trade_status'] = 0
            # try:datetime.strptime(data.iloc[k]['DATE'], "%Y-%m-%d %H:%M:%S")
            bar_ = bar.BasicBar(dateTime=datetime.strptime(data.iloc[k]['DATE'][0:19], "%Y-%m-%d %H:%M:%S"),
                                open_=data.iloc[k]['OPEN'], high=data.iloc[k]['HIGH'],
                                low=data.iloc[k]['LOW'], close=data.iloc[k]['CLOSE'], volume=data.iloc[k]['VOLUME'],
                                adjClose=data.iloc[k]['ADJFACTOR'] * data.iloc[k]['CLOSE'], frequency=bar.Frequency.DAY,
                                extra=extrabar)
            # except Exception:
            #     continue
            self.addBar(instrument, bar_, bar_.getFrequency())

    def importBarsFromCSVByPandas(self, filename, instrument):
        def tradeStatusConverter(tradeStatus):
            if tradeStatus == u'交易':
                ret = 1
            else:
                ret = 0
            return ret

        from skywalker.utils.dt import datetime_to_timestamp
        dateConverter = lambda date: datetime_to_timestamp(datetime.strptime(date[0:19], "%Y-%m-%d %H:%M:%S"))
        converters = {'TRADE_STATUS': tradeStatusConverter}
        data = pd.read_csv(filename, encoding='gbk', converters=converters)
        data['adj_close'] = data['ADJFACTOR'] * data['CLOSE']
        data['instrument_id'] = self.__getOrCreateInstrument(instrument)
        data['timestamp'] = data['DATE'].apply(dateConverter)
        data['frequency'] = bar.Frequency.DAY
        data = data.drop(['DATE', 'ADJFACTOR'], axis=1)
        data = data.dropna()
        data.columns = data.columns.map(lambda x: x.lower())
        engine = self.__engine
        data.to_sql(name='bar', con=engine, if_exists='append', index=False)


    def addBar(self, instrument, bar, frequency):
        """将bar中数据插入数据库"""
        instrument = normalize_instrument(instrument)
        instrumentId = self.__getOrCreateInstrument(instrument)
        timeStamp = int(dt.datetime_to_timestamp(bar.getDateTime()))
        try:
            # extra为空时
            if not bar.getExtraColumns():
                sql = "insert into bar (instrument_id, frequency, timestamp, open, high, low, close, volume, adj_close) values (%s, %s, %s, %s, %s, %s, %s,%s, %s)"
                params = [instrumentId, frequency, timeStamp, bar.getOpen(), bar.getHigh(), bar.getLow(),
                          bar.getClose(), bar.getVolume(), bar.getAdjClose()]
                self.__cursor.execute(sql, params)
                self.__connection.commit()
            else:
                # extra非空时，将extra中字段处理为逗号分隔的连续字符串形式，用于执行sql语句
                extraColumns = bar.getExtraColumns()
                keys = list(extraColumns.keys())
                delimiter = ','
                keysString = delimiter.join(keys)
                for x in range(0, len(keys)):
                    if keys[x] not in self.getBarFields():
                        # 字段不存在则先添加字段再插入数据
                        sqlAlter = '''ALTER TABLE bar ADD COLUMN {field} numeric '''
                        self.__cursor.execute(sqlAlter.format(field=keys[x]))
                        self.__connection.commit()
                sql1 = "insert into bar (instrument_id, frequency, timestamp, open, high, low, close, volume, adj_close," + keysString + ")"
                # %s"*len(keys)表示需要额外传入的参数个数
                sql2 = " values (%s, %s, %s, %s, %s, %s, %s, %s, %s" + " ,%s" * len(keys) + ")"
                sql = sql1 + sql2
                params1 = [instrumentId, frequency, timeStamp, bar.getOpen(), bar.getHigh(), bar.getLow(),
                           bar.getClose(), bar.getVolume(), bar.getAdjClose()]
                params2 = list(extraColumns.values())
                params2 = list(map(self.nanToNone, params2))
                params = params1 + params2
                self.__cursor.execute(sql, params)
                self.__connection.commit()
        except psycopg2.IntegrityError:
            self.__connection.rollback()
            keys = list(bar.getExtraColumns().keys())
            sql = "update bar set open = %s, high = %s, low = %s, close = %s, volume = %s, adj_close = %s{extra}" \
                  " where instrument_id = %s and frequency = %s and timestamp = %s"
            extraUpdate = ''
            for i in range(0, len(keys)):
                extraUpdate = extraUpdate + ', ' + keys[i] + ' = %s'
            params1 = [bar.getOpen(), bar.getHigh(), bar.getLow(), bar.getClose(), bar.getVolume(), bar.getAdjClose()]
            params2 = [instrumentId, frequency, timeStamp]
            extraParams = []
            for i in range(0, len(keys)):
                extraParams.append(bar.getExtraColumns()[keys[i]])
            params = params1 + extraParams + params2
            self.__cursor.execute(sql.format(extra=extraUpdate), params)
            self.__connection.commit()

    def nanToNone(self, param):
        if isnan(param):
            return None
        else:
            return param

    def getBars(self, instrument, frequency, timezone=None, fromDateTime=None, toDateTime=None, extra=None):
        instrument = normalize_instrument(instrument)
        sql = "select bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.adj_close, bar.frequency{extra}" \
              " from bar join instrument on (bar.instrument_id = instrument.instrument_id)" \
              " where instrument.name = %s and bar.frequency = %s"
        extraFieldsString = ''
        if extra is None:
            extra = []
        for i in range(0, len(extra)):
            extraFieldsString = extraFieldsString + ", " + "bar." + extra[i]
        args = [instrument, frequency]
        if fromDateTime is not None:
            sql += " and bar.timestamp >= %s"
            args.append(dt.datetime_to_timestamp(fromDateTime))
        if toDateTime is not None:
            sql += " and bar.timestamp <= %s"
            args.append(dt.datetime_to_timestamp(toDateTime))
        sql += " order by bar.timestamp asc"
        cursor = self.__connection.cursor()
        cursor.execute(sql.format(extra=extraFieldsString), args)
        self.__connection.commit()
        ret = []
        for row in cursor:
            dateTime = dt.timestamp_to_datetime(row[0])
            if timezone:
                dateTime = dt.localize(dateTime, timezone)
            extraRow = {}
            for i in range(0, len(extra)):
                extraRow[extra[i]] = row[8 + i]
            ret.append(bar.BasicBar(dateTime, row[1], row[2], row[3], row[4], row[5], row[6], row[7], extra=extraRow))
        cursor.close()
        return ret

    def disconnect(self):
        self.__connection.close()
        self.__connection = None

    def getBarFields(self):
        """ 返回表格的字段"""
        sql = '''SELECT a.attname as name
        FROM pg_class as c,pg_attribute as a
        where c.relname = 'bar' and a.attrelid = c.oid and a.attnum>0  '''
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        fields = cursor.fetchall()
        fields = [tup[0] for tup in fields]
        # fields = list(zip(*fields)[0])
        # 删除某个字段后postgreSQL数据库中会留下类似........pg.dropped.9........的字段，需要将其去除
        fields = [i for i in fields if '........' not in i]
        return fields

    def dropField(self, field):
        '''删除某个字段'''
        if field not in self.getBarFields():
            return False
        else:
            sqldrop = '''ALTER TABLE {tb} DROP COLUMN {field}'''
            cursor = self.__connection.cursor()
            cursor.execute(sqldrop.format(tb="bar", field=field))
            self.__connection.commit()
            return True

    def truncateAllTalbes(self):
        truncateSQL = '''TRUNCATE TABLE {tb} CASCADE'''
        cursor = self.__connection.cursor()
        cursor.execute(truncateSQL.format(tb="bar"))
        self.__connection.commit()
        cursor.execute(truncateSQL.format(tb="instrument"))
        self.__connection.commit()


class Feed(membf.BarFeed):
    def __init__(self, frequency=bar.Frequency.DAY, host='localhost', port=5432, user='postgres', password='123456',
                 database='Skywalker', maxLen=None):
        super(Feed, self).__init__(frequency, maxLen)
        self.__db = Database(host=host, port=port, user=user, password=password, database=database)
        self.__equityEventsDict = None

    def barsHaveAdjClose(self):
        return True

    def getDatabase(self):
        return self.__db

    def loadBars(self, instrument, timezone=None, fromDateTime=None, toDateTime=None, extra=None):
        if extra is None:
            extra = []
        fd = datetime.strptime(fromDateTime, '%Y-%m-%d')
        td = datetime.strptime(toDateTime, '%Y-%m-%d')
        for i in range(0, len(instrument)):
            bars = self.__db.getBars(instrument[i], self.getFrequency(), timezone, fd, td, extra=extra)
            self.addBarsFromSequence(instrument[i], bars)
        self.__equityEventsDict = {k: self.__getEquityEvent(k) for k in instrument}

    # def importBars(self, instrument, fromDateTime, toDateTime, extra=[]):
    #     self.__db.importBars(instrument, fromDateTime, toDateTime, extra)

    def getAllInstruments(self):
        instruments = self.__db.getAllInstruments()
        return instruments

    def __getEquityEvent(self, instrument):
        ee = self.__db.getEquityEvent(instrument)
        return ee

    def getEquityEvent(self, instrument):
        try:
            return self.__equityEventsDict[instrument]
        except TypeError:
            return None

    def haveEquityEvent(self, instrument, date):
        equityEvent = self.getEquityEvent(instrument)
        if equityEvent is None or len(equityEvent[date:date]) <= 0:
            haveEE = False
        else:
            haveEE = True
        return haveEE
