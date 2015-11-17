import os, sys
sys.path.append(os.path.abspath('placementstrategy'))
from PlacementFactory import PlacementFactory
from HistoricalNode import HistoricalNode
from Node import Node

class Coordinator(Node):
	
	@staticmethod
	def placeSegmentsAndReplicas(segmentList, replicationFactor, historicalNodeList, queryList, placementStrategy):
        	strategy = PlacementFactory.createPlacementStrategy(placementStrategy)
		numsegments = 0
	        (segments,unique) = strategy.placeSegments(segmentList, historicalNodeList, queryList)
		numsegments += segments
		numsegments += strategy.replicateSegments(segmentList, replicationFactor, historicalNodeList);

		print("NumSegment: %d Segement Count:%d" % (numsegments,unique))
		return float(numsegments)/unique

	@staticmethod
	def printCurrentPlacement(historicalNodeList):
		for node in historicalNodeList:
			node.printSegmentList()
