import math
import numpy
import sys
from HistoricalNode import HistoricalNode

class DruidStrategy(object):
	def routeQueries(self, queryList, historicalNodeList, segmentCount):
		segmentmap = dict()
		for i in range(1, segmentCount + 1):
			segmentmap[i] = list()

		for node in historicalNodeList:
			for segment in node.getSegmentAndReplicaList():
				segmentmap[segment.getTime()].append(node.getID())
				 
		queryassgn = dict()
		for x in range (1, len(historicalNodeList)+1):
			queryassgn[x] = 0
			
		for query in queryList:
			for segment in query.segmentList:
				hnindex = self.getHistoricalNode(segmentmap, segment, queryassgn)
				queryassgn[hnindex] += 1

		maxscore = 0
		target = None
		for node in historicalNodeList:
			if queryassgn[node.getID()] >= maxscore:
				maxscore = queryassgn[node.id]
				target = node.getID()

		return maxscore

class Random(DruidStrategy):
	def getHistoricalNode(self, segmentMap, segment, queryassgn):
		numreplicas = len(segmentMap[segment])
		index = numpy.random.random_integers(1, numreplicas)
		return segmentMap[segment][index - 1]
		
class ChooseLeastLoaded(DruidStrategy):
	def getHistoricalNode(self, segmentMap, segment, queryassgn):
		leastload = sys.maxint
		leastloadednode = 0
		for histnode in segmentMap[segment]:
			if queryassgn[histnode] < leastload:
				leastload = queryassgn[histnode]
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
