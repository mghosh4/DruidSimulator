import os, sys
sys.path.append(os.path.abspath('placementstrategy'))
from PlacementFactory import PlacementFactory
from HistoricalNode import HistoricalNode
from Node import Node

class Coordinator(Node):
	
	@staticmethod
	def placeSegments(segmentlist, historicalnodelist, placementstrategy):
        	strategy = PlacementFactory.createPlacementStrategy(placementstrategy)
	        strategy.placeSegments(segmentlist, historicalnodelist)

	@staticmethod
	def printCurrentPlacement(historicalNodeList):
		for node in historicalNodeList:
			node.printSegmentList()
