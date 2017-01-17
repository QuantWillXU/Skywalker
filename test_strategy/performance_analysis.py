import cProfile
from samplestrat import VWAPMomentum
from sample import MyStrategy


# def runAll():
#     strat = VWAPMomentum()
#     strat.setStartDate("2015-09-01")
#     strat.setEndDate("2016-08-30")
#     strat.setUniverse(['000005.SZ', '000025.SZ', '600128.SH'])
#     strat.setBenchmenk('000001.SH')
#     strat.setUseAdjustedValues(True)
#     strat.run(False)
#
# cProfile.run('runAll()',"result")
# import pstats
# p = pstats.Stats("result")
# p.strip_dirs().sort_stats('tottime').print_stats(0.1)


# def runAll():
#     strat = MyStrategy()
#     strat.setStartDate("2015-06-26")
#     strat.setEndDate("2016-07-30")
#     strat.setUniverse(["300345.SZ"])
#     strat.setBenchmenk('000001.SH')
#     strat.run(True)
#
# cProfile.run('runAll()',"result")
import pstats
p = pstats.Stats("strat_run.prof")
p.strip_dirs().sort_stats('tottime').print_stats(0.1)




