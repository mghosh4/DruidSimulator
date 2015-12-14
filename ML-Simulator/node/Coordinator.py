import os, sys
sys.path.append(os.path.abspath('placementstrategy'))
from PlacementFactory import PlacementFactory
from HistoricalNode import HistoricalNode
from Node import Node

class Coordinator(Node):
	
	@staticmethod
	def placeSegmentsAndReplicas(segmentList, deepStorage, percentreplicate, replicationFactor, historicalNodeList, queryList, placementStrategy):
        	strategy = PlacementFactory.createPlacementStrategy(placementStrategy)
		numsegments = 0
	        (segments,unique) = strategy.placeSegments(segmentList, deepStorage, historicalNodeList, queryList)
		numsegments += segments
		numsegments += strategy.replicateSegments(deepStorage, percentreplicate, replicationFactor, historicalNodeList, queryList);

		#print("NumSegment: %d Segment Count:%d" % (numsegments,unique))
		return float(numsegments)/unique
