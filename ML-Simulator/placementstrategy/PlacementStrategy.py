import numpy
import operator
import math
import sys, os
sys.path.append(os.path.abspath('utilities'))
from HistoricalNode import HistoricalNode
from Utils import Utils

class PlacementStrategy(object):

    def placeSegments(self, segmentList, historicalNodeList, queryList):
	for segment in segmentList:
		#print "=== Placing segment %d ===" % segment.time
		historicalNodeIndex = self.getNextIndex(segment, historicalNodeList)
		#print "Primary node %d" % historicalNodeList[historicalNodeIndex-1].id
		historicalNodeList[historicalNodeIndex-1].add_segment(segment)
	return (len(segmentList), len(segmentList))

    def replicateSegments(self, segmentList, replicationFactor, historicalNodeList):
	for segment in segmentList:
		for _ in xrange(replicationFactor - 1):
			historicalNodeIndex = self.getNextIndex(segment, historicalNodeList)		
			historicalNodeList[historicalNodeIndex-1].add_replica(segment)
	return (replicationFactor - 1) * len(segmentList)

#TODO: getNextIndex should not return a hn index which contains the segment already
#class Random(PlacementStrategy):
#    def getNextIndex(self, segment, historicalNodeList):
#        return numpy.random.random_integers(1, len(historicalNodeList))

class DruidCostBased(PlacementStrategy):
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

class RandomBallBased(object):
    def placeSegments(self, segmentList, historicalNodeList, queryList):
	count = 0
	allquerysegments = list()
	for query in queryList:
		allquerysegments.extend(query.segmentList)
		for segment in query.segmentList:
        		hnindex = numpy.random.random_integers(1, len(historicalNodeList))
			if historicalNodeList[hnindex - 1].lookup(segment) == False:
				count += 1
			historicalNodeList[hnindex - 1].add_segment(segmentList[segment - 1])

	uniqueset = set(allquerysegments)
	return (count,len(uniqueset))

    def replicateSegments(self, segmentList, replicationFactor, historicalNodeList):
	return 0

class BestFit(object):
    def placeSegments(self, segmentList, historicalNodeList, queryList):
	segmentmap = dict()
	segmentcount = len(segmentList)
	for i in xrange(1,segmentcount + 1):
		segmentmap[i] = 0

	totalslots = 0
	allquerysegments = list()
	for query in queryList:
		allquerysegments.extend(query.segmentList)
		for segment in query.segmentList:
			segmentmap[segment] += 1
			totalslots += 1

	filtermap = dict(filter(lambda x: x[1] != 0, segmentmap.items()))
	sorted_dict = sorted(filtermap.items(), key=operator.itemgetter(1), reverse=True)

	historicalnodecount = len(historicalNodeList)
	slotsperhn = math.ceil(float(totalslots) / historicalnodecount)
	#print slotsperhn, totalslots
	hnindex = 0
	capacity = slotsperhn
	numsegment = 0
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
			numsegment += 1

	uniqueset = set(allquerysegments)
	return (numsegment,len(uniqueset))

    def replicateSegments(self, segmentList, replicationFactor, historicalNodeList):
	return 0
