import sys,os
import numpy
sys.path.append(os.path.abspath('../node'))
from HistoricalNode import HistoricalNode

class PlacementStrategy(object):
    def placeSegments(self, segmentList, historicalNodeList):
        for segment in segmentList:
            historicalNodeIndex = self.getNextIndex(segment, historicalNodeList)
            historicalNodeList[historicalNodeIndex].add_segment(segment)

class Random(PlacementStrategy):
    def getNextIndex(self, segment, historicalNodeList):
        return numpy.random.random_integers(0, len(historicalNodeList))


class DruidCostBased(PlacementStrategy):
    def getNextIndex(self, segment, historicalNodeList):
        min = historicalNodeList[0].calculateDruidCost()
        lowestcost = 0
        for hn in historicalNodeList:
            cost = hn.calculateDruidCost()
            if min > cost:
                min = cost
                lowestcost = hn.getID()

        return lowestcost