import os, sys
from collections import Counter
sys.path.append(os.path.abspath('placementstrategy'))
sys.path.append(os.path.abspath('replicationstrategy'))
sys.path.append(os.path.abspath('utilities'))

from PlacementFactory import PlacementFactory
from ReplicationFactory import ReplicationFactory
from HistoricalNode import HistoricalNode
from Node import Node
from Utils import Utils

class Coordinator(Node):
	@staticmethod
	def placeSegmentsAndReplicas(segmentList, deepStorage, historicalNodeList, queryList, placementStrategy, replicationStrategy, segmentCount):
        	placementstrategy = PlacementFactory.createPlacementStrategy(placementStrategy)
        	replicationstrategy = ReplicationFactory.createReplicationStrategy(replicationStrategy)
		
		## Placing Segments
	        placementstrategy.placeSegments(segmentList, historicalNodeList)
		Coordinator.updateSegmentCount(segmentList, segmentCount)

		## Replicating Segments
		(insertlist, removelist) = replicationstrategy.replicateSegments(segmentList, deepStorage, historicalNodeList, queryList, segmentCount)
		#Utils.printSegmentList(insertlist)
		#Utils.printSegmentList(removelist)

		placementstrategy.removeSegments(removelist, historicalNodeList)
	        placementstrategy.placeSegments(insertlist, historicalNodeList)
		Coordinator.updateSegmentCount(insertlist, segmentCount)
		Coordinator.removeSegmentCount(removelist, segmentCount)

	@staticmethod
	def updateSegmentCount(segmentList, segmentCount):
		for segment in segmentList:
			segmentCount[segment] += 1

	@staticmethod
	def removeSegmentCount(segmentList, segmentCount):
		for segment in segmentList:
			if segmentCount[segment] > 0:
				segmentCount[segment] -= 1
