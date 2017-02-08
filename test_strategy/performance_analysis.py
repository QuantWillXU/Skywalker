import pstats
p = pstats.Stats("strat_run.prof")
p.strip_dirs().sort_stats('tottime').print_stats(0.1)




