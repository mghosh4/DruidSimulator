import math
import numpy
import sys
from HistoricalNode import HistoricalNode

class DruidStrategy(object):
	def routeQueries(self, queryList, historicalNodeList, segmentCount, startTime):
		segmentmap = dict()
		for query in queryList:
		    segmentlist = query.getSegmentList()
		    for segment in segmentlist.keys():
			if segment not in segmentmap:
			    segmentmap[segment] = list()
			    for node in historicalNodeList:
			    	if node.lookupBySegment(segment) == True:
				    segmentmap[segment].append(node)

		for query in queryList:
			maxtime = 0
			for segment in query.getSegmentList().keys():
				hnode = self.getHistoricalNode(segmentmap, segment, historicalNodeList)
				endtime = hnode.routeQuery(query, startTime)
				if endtime > maxtime:
					maxtime = endtime
			query.setEndTime(maxtime)


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
