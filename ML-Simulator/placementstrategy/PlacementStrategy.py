import numpy
import operator
import math
import sys, os
sys.path.append(os.path.abspath('utilities'))
from HistoricalNode import HistoricalNode
from Utils import Utils

class PlacementStrategy(object):

    def placeSegments(self, segmentList, deepStorage, historicalNodeList, queryList):
	for segment in segmentList:
	    #print "=== Placing segment %d ===" % segment.time
	    historicalNodeIndex = self.getNextIndex(segment, historicalNodeList)
	    #print "Primary node %d" % historicalNodeList[historicalNodeIndex-1].id
	    historicalNodeList[historicalNodeIndex-1].add_segment(segment)
	return (len(segmentList), len(segmentList))

    def replicateSegments(self, deepStorage, percentreplicate, replicationFactor, historicalNodeList, queryList):
	querysegmentmap = dict()
	for query in queryList:
	    segmentcountmap = query.getSegmentCount()
	    for segment,count in segmentcountmap.items():
	    	replicacount = 0
	    	for hn in historicalNodeList:
                    if hn.lookup(segment) == True:
	    	        replicacount += 1

	    	if replicacount >= 2:
		    continue

                if segment not in querysegmentmap:
                    querysegmentmap[segment] = 0
                querysegmentmap[segment] += count

	sorted_dict = sorted(querysegmentmap.items(), key=operator.itemgetter(1), reverse=True)
	
	numreplicate = math.ceil(percentreplicate * len(sorted_dict))
	print "Replicate Segments" 
	print sorted_dict
	print "Number to replicated: %d" % numreplicate

	replicatedcount = 0
	for segment,count in sorted_dict:
            if self.isReplicated(segment, historicalNodeList) == False:
                for _ in xrange(replicationFactor - 1):
		    historicalNodeIndex = self.getNextIndex(deepStorage[segment - 1], historicalNodeList)		
		    historicalNodeList[historicalNodeIndex-1].add_replica(deepStorage[segment - 1])
	    replicatedcount += 1
	    if replicatedcount >= numreplicate:
		break

	return (replicationFactor - 1) * numreplicate

    def isReplicated(self, segment, historicalNodeList):
        replicacount = 0
        for node in historicalNodeList:
            if node.lookup(segment) == True:
                replicacount += 1

        return (replicacount > 1)

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
    def placeSegments(self, segmentList, deepStorage, historicalNodeList, queryList):
	count = 0
	allquerysegments = list()
	for query in queryList:
		allquerysegments.extend(query.segmentList)
		for segment in query.segmentList:
        		hnindex = numpy.random.random_integers(1, len(historicalNodeList))
			if historicalNodeList[hnindex - 1].lookup(segment) == False:
				count += 1
			historicalNodeList[hnindex - 1].add_segment(deepStorage[segment - 1])

	uniqueset = set(allquerysegments)
	return (count,len(uniqueset))

    def replicateSegments(self, deepStorage, percentReplicate, replicationFactor, historicalNodeList, queryList):
	return 0

class BestFit(object):
    def placeSegments(self, segmentList, deepStorage, historicalNodeList, queryList):
	querysegmentmap = dict()
	totalslots = 0
	allquerysegments = list()
	for query in queryList:
	    segmentcountmap = query.getSegmentCount()
	    allquerysegments.extend(query.segmentList)
	    for segment,count in segmentcountmap.items():
	    	replicacount = 0
	    	for hn in historicalNodeList:
                    if hn.lookup(segment) == True:
	    	        replicacount += 1
			break

	    	if replicacount >= 1:
		    continue

                if segment not in querysegmentmap:
                    querysegmentmap[segment] = 0
                querysegmentmap[segment] += count
		totalslots += count

	sorted_dict = sorted(querysegmentmap.items(), key=operator.itemgetter(1), reverse=True)

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
				historicalNodeList[hnindex].add_segment(deepStorage[key - 1])
				val = val - capacity
				hnindex = hnindex + 1
				capacity = slotsperhn
			else:
				historicalNodeList[hnindex].add_segment(deepStorage[key - 1])
				capacity = capacity - val
				val = 0
			numsegment += 1

	uniqueset = set(allquerysegments)
	return (numsegment,len(uniqueset))

    def replicateSegments(self, segmentList, percentReplicate, replicationFactor, historicalNodeList, queryList):
	return 0
