import os, sys
sys.path.append(os.path.abspath('placementstrategy'))
from PlacementFactory import PlacementFactory
from HistoricalNode import HistoricalNode
from Node import Node

class Coordinator(Node):
	
	@staticmethod
	def placeSegments(segmentList, historicalNodeList, queryList, placementStrategy):
        	strategy = PlacementFactory.createPlacementStrategy(placementStrategy)
	        strategy.placeSegments(segmentList, historicalNodeList, queryList)

	@staticmethod
	def printCurrentPlacement(historicalNodeList):
		for node in historicalNodeList:
			node.printSegmentList()
