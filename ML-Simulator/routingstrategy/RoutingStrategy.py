import math
import numpy
import sys
from HistoricalNode import HistoricalNode

class DruidStrategy(object):
	def routeQueries(self, queryList, historicalNodeList, segmentCount, startTime):
		segmentmap = dict()
		for query in queryList:
		    segmentcountmap = query.getSegmentCount()
		    for segment,count in segmentcountmap.items():
			if segment not in segmentmap:
			    for node in historicalNodeList:
			    	if node.lookup(segment) == True:
				    if segment not in segmentmap:
					segmentmap[segment] = list()
				    segmentmap[segment].append(node)

		for query in queryList:
			for segment in query.segmentList:
				hnode = self.getHistoricalNode(segmentmap, segment, historicalNodeList)
				hnode.routeQuery(query, startTime)

		maxtime = 0
		target = None
		for node in historicalNodeList:
			if node.computeEndsAt() >= maxtime:
				maxtime = node.computeEndsAt()
				target = node.getID()

		return maxtime

class Random(DruidStrategy):
	def getHistoricalNode(self, segmentMap, segment, historicalNodeList):
		numreplicas = len(segmentMap[segment])
		index = numpy.random.random_integers(1, numreplicas)
		return segmentMap[segment][index - 1]
		
class ChooseLeastLoaded(DruidStrategy):
	def getHistoricalNode(self, segmentMap, segment, historicalNodeList):
		leastload = sys.maxint
		leastloadednode = 0
		for histnode in segmentMap[segment]:
			if histnode.computeEndsAt() < leastload:
				leastload = histnode.computeEndsAt()
				leastloadednode = histnode
		return leastloadednode
		
class RandomBallBased(object):
	def routeQueries(self, queryList, historicalNodeList, segmentCount):
		maxLoad = 0
		for node in historicalNodeList:
			if maxLoad < node.queue_size():
				maxLoad = node.queue_size()

		return maxLoad
		
class BestFit(object):
	def routeQueries(self, queryList, historicalNodeList, segmentCount):
		totalslots = 0
		for query in queryList:
			for segment in query.segmentList:
				totalslots += 1

		return math.ceil(float(totalslots) / len(historicalNodeList))
