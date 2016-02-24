import math
import numpy
import sys
from HistoricalNode import HistoricalNode

class DruidStrategy(object):
	def routeQueries(self, queryList, historicalNodeList, segmentCount, startTime):
		segmentmap = dict()
		for query in queryList:
		    segmenttimecount = query.getSegmentTimeCount()
		    for time,count in segmenttimecount.items():
			if time not in segmentmap:
			    for node in historicalNodeList:
			    	if node.lookupByTime(time) == True:
				    if time not in segmentmap:
					segmentmap[time] = list()
				    segmentmap[time].append(node)

		for query in queryList:
			for time in query.segmentTimeList:
				hnode = self.getHistoricalNode(segmentmap, time, historicalNodeList)
				hnode.routeQuery(query, startTime)

		maxtime = 0
		target = None
		for node in historicalNodeList:
			if node.computeEndsAt() >= maxtime:
				maxtime = node.computeEndsAt()
				target = node.getID()

		return maxtime

class Random(DruidStrategy):
	def getHistoricalNode(self, segmentMap, time, historicalNodeList):
		numreplicas = len(segmentMap[time])
		index = numpy.random.random_integers(1, numreplicas)
		return segmentMap[time][index - 1]
		
class ChooseLeastLoaded(DruidStrategy):
	def getHistoricalNode(self, segmentMap, time, historicalNodeList):
		leastload = sys.maxint
		leastloadednode = 0
		for histnode in segmentMap[time]:
			if histnode.computeEndsAt() < leastload:
				leastload = histnode.computeEndsAt()
				leastloadednode = histnode
		return leastloadednode
