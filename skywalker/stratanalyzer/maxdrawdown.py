import pandas as pd

from skywalker.stratanalyzer.drawdown import DrawDownHelper


def calculateMaxDrawDown(monthIndex, n, dateList, startEndList, equityList):
    if monthIndex < n - 1:
        return 'NaN'
    else:
        startDay = startEndList[monthIndex - n + 1][0]
        endDay = startEndList[monthIndex][1]
        df = pd.DataFrame(data=equityList, index=dateList, columns=['Equity'])
        equityBetween = df[startDay:endDay]['Equity']
        maxDrawDown = 0.0
        maxDrawDownDay = None
        currDrawDown = DrawDownHelper()

        for i in equityBetween.index:
            currDrawDown.update(i, equityBetween[i], equityBetween[i])
            if currDrawDown.getCurrentDrawDown() <= maxDrawDown:
                maxDrawDownDay = i
                maxDrawDown = min(maxDrawDown, currDrawDown.getMaxDrawDown())

        maxDrawDownDay = maxDrawDownDay.strftime('%Y-%m-%d')
        maxDDPlusDay = str(-maxDrawDown) + '(' + maxDrawDownDay + ')'
        return maxDDPlusDay
