import numpy
import operator
import math
import sys
import os.path
from HistoricalNode import HistoricalNode
from DruidReplicationStrategy import DruidReplicationStrategy

class PlacementStrategy(object):

    def placeSegments(self, segmentList, historicalNodeList, queryList):
		repList = []
		for segment in segmentList:
			#print "=== Placing segment %d ===" % segment.time
			historicalNodeIndex = self.getNextIndex(segment, historicalNodeList)
			#print "Primary node %d" % historicalNodeList[historicalNodeIndex-1].id
			historicalNodeList[historicalNodeIndex-1].add_segment(segment)
			repidList = DruidReplicationStrategy.replicateSegments(historicalNodeList, historicalNodeList[historicalNodeIndex-1], "balance", 3)
			for hn in historicalNodeList:
				for rpid in repidList:
					if hn.id == rpid:
						#print "Replica node %d" % rpid
						hn.add_replica(segment)
			

class Random(PlacementStrategy):
    def getNextIndex(self, segment, historicalNodeList):
        return numpy.random.random_integers(1, len(historicalNodeList))

class DruidCostBased(PlacementStrategy):
    def getNextIndex(self, segment, historicalNodeList):
        min = historicalNodeList[0].calculateDruidCost()
        lowestcost = 1
        for hn in historicalNodeList:
            cost = hn.calculateDruidCost()
            if min > cost:
                min = cost
                lowestcost = hn.getID()

        return lowestcost

class BestFit(object):
    def placeSegments(self, segmentList, historicalNodeList, queryList):
	segmentmap = dict()
	segmentcount = len(segmentList)
	for i in xrange(1,segmentcount + 1):
		segmentmap[i] = 0

	totalslots = 0
	for query in queryList:
		for segment in query.segmentList:
			segmentmap[segment] += 1
			totalslots += 1

	filtermap = dict(filter(lambda x: x[1] != 0, segmentmap.items()))
	sorted_dict = sorted(filtermap.items(), key=operator.itemgetter(1), reverse=True)

	historicalnodecount = len(historicalNodeList)
	slotsperhn = math.ceil(float(totalslots) / historicalnodecount)
	print slotsperhn, totalslots
	hnindex = 0
	capacity = slotsperhn
	for key,val in sorted_dict:
		#print key,":",val
		while val > 0:
			if val >= capacity:
				historicalNodeList[hnindex].add_segment(segmentList[key - 1])
				val = val - capacity
				hnindex = hnindex + 1
				capacity = slotsperhn
			else:
				historicalNodeList[hnindex].add_segment(segmentList[key - 1])
				capacity = capacity - val
				val = 0

