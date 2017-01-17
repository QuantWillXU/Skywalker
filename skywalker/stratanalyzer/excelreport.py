# -*- coding:utf-8 -*-
import os

from PIL import Image
from xlwt import *


class ExcelReport:
    def __init__(self, reportname):
        self.__reportname = reportname
        self.__report = Workbook(encoding='utf-8')
        self.__sheetDict = {}

    def addSheet(self, sheetname, cell_overwrite_ok=False):
        """添加工作表"""
        ret = self.__report.add_sheet(sheetname, cell_overwrite_ok=cell_overwrite_ok)
        self.__sheetDict[sheetname] = ret
        return ret

    def setColWidth(self, sheetname, colnumber, width):
        """设置列宽，width表示字符数"""
        if sheetname not in self.__sheetDict.keys():
            return
        else:
            self.__sheetDict[sheetname].col(colnumber).width = width * 256

    def write(self, sheetname, x, y, content, width=None):
        """向指定工作表的指定单元格插入数据"""
        sheet = self.__sheetDict[sheetname]
        # print "write x:%f y:%f" % (x, y)
        # print content
        # print type(content)
        sheet.write(y, x, content)
        if width is not None:
            self.setColWidth(sheetname, x, width)

    def writeList(self, sheetname, x, y, list, horzOrVert=1):
        """将列表插入工作表中，x和y表示插入起始点的坐标，horzOrVert=1表示垂直插入，horzOrVert=0表示水平插入"""
        if horzOrVert == 0:
            for i in range(0, len(list)):
                self.write(sheetname, x + i, y, list[i])
        elif horzOrVert == 1:
            for i in range(0, len(list)):
                self.write(sheetname, x, y + i, list[i])

    def writeListDict(self, sheetname, x, y, dict, horzOrVert=0, keys=None):
        """插入列表字典，即字典的每个值为一个列表，x和y表示插入起始点坐标，horzOrVert表示水平或竖直插入"""
        if keys is None:
            keys = dict.keys()
        if horzOrVert == 1:
            for i in range(0, len(keys)):
                self.write(sheetname, x + i, y, keys[i])
                self.writeList(sheetname, x + i, y + 1, dict[keys[i]], 1)
        elif horzOrVert == 0:
            for i in range(0, len(keys)):
                self.write(sheetname, x, y + i, keys[i])
                self.writeList(sheetname, x + 1, y + i, dict[keys[i]], 0)

    def insertPicture(self, sheetname, x, y, picname, scale_x=1, scale_y=1):
        """插入图片，x和y表示插入坐标， scale_x=1, scale_y=1表示相对于原图的比例"""
        sheet = self.__sheetDict[sheetname]
        fig = Image.open(picname)
        fig.save("temp.bmp")
        sheet.insert_bitmap("temp.bmp", x, y, scale_x=scale_x, scale_y=scale_y)
        if os.path.exists("temp.bmp"):
            os.remove("temp.bmp")

    def save(self):
        """保存报告"""
        self.__report.save(self.__reportname + '.xls')
        # self.__report.save(u'D:\\report.xls')
