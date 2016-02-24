import numpy
import operator
import math
import sys, os
sys.path.append(os.path.abspath('utilities'))
from HistoricalNode import HistoricalNode
from Utils import Utils

class PlacementStrategy(object):

    def placeSegments(self, segmentList, historicalNodeList):
	for segment in segmentList:
	    #print "=== Placing segment %d ===" % segment.time
	    historicalNodeIndex = self.getNextIndex(segment, historicalNodeList)
	    #print "Primary node %d" % historicalNodeList[historicalNodeIndex-1].id
	    historicalNodeList[historicalNodeIndex-1].addSegment(segment)

    def removeSegments(self, segmentList, historicalNodeList):
	for segment in segmentList:
	    #print "=== Placing segment %d ===" % segment.time
	    historicalNodeIndex = self.removeFromIndex(segment, historicalNodeList)
	    #print "Primary node %d" % historicalNodeList[historicalNodeIndex-1].id
	    historicalNodeList[historicalNodeIndex-1].removeSegment(segment)

#TODO: getNextIndex should not return a hn index which contains the segment already
#class Random(PlacementStrategy):
#    def getNextIndex(self, segment, historicalNodeList):
#        return numpy.random.random_integers(1, len(historicalNodeList))

class DruidCostBased(PlacementStrategy):
    def removeFromIndex(self, segment, historicalNodeList):
        highestload = 0
	maxloadnode = -1
        for hn in historicalNodeList:
            if hn.lookupBySegment(segment) == True:
		if (highestload < hn.numReplicasPlaced()):
		    highestload = hn.numReplicasPlaced
		    maxloadnode = hn.getID()

        return maxloadnode

    def getNextIndex(self, segment, historicalNodeList):
        lowestcost = sys.maxsize
	mincostnode = -1
        for hn in historicalNodeList:
	    cost = 0
	    for hnsegment in hn.getSegmentAndReplicaList():
		cost += self.calculateDruidCost(segment, hnsegment)
            if lowestcost > cost:
                lowestcost = cost
                mincostnode = hn.getID()

        return mincostnode

    def calculateDruidCost(self, newsegment, hnsegment):
	if newsegment.time == hnsegment.time:
		return sys.maxsize

	basecost = min(newsegment.size, hnsegment.size)
	datasourcepenalty = 1
	recencypenalty = 1
	gappenalty = 1
	if newsegment.time < Utils.RECENCY_THRESHOLD and hnsegment.time < Utils.RECENCY_THRESHOLD:
		recencypenalty = 2

	if newsegment.time - hnsegment.time < Utils.GAP_THRESHOLD:
		gappenalty = 2 - (newsegment.time - hnsegment.time)/Utils.GAP_THRESHOLD

	return basecost * datasourcepenalty * recencypenalty * gappenalty
