import os, sys
sys.path.append(os.path.abspath('routingstrategy'))
from RoutingFactory import RoutingFactory
from Node import Node

class Broker(Node):     
	
	@staticmethod
	def routeQueries(queryList, historicalNodeList, placementStrategy, segmentcount):
		strategy = RoutingFactory.createRoutingStrategy(placementStrategy)
		return strategy.routeQueries(queryList, historicalNodeList, segmentcount)
