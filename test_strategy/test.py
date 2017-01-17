from WindPy import *
w.start()
data = w.wsd("300345.SZ", "div_capitalization,div_stock,div_progress,div_cashandstock,"
                          "div_exdate,div_cashaftertax,div_cashbeforetax2", "2015-06-25",
             "2015-07-02", "currencyType=BB;Fill=Previous")
# print type(data.Data[0][0])