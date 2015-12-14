import os,sys
sys.path.append(os.path.abspath('node'))
sys.path.append(os.path.abspath('utilities'))

from HistoricalNode import HistoricalNode
from Coordinator import Coordinator
from Broker import Broker
from Utils import Utils

class Strategy(object):
	def __init__(self, historicalNodeCount, placementStrategy, routingStrategy):
		self.placementStrategy = placementStrategy
		self.routingStrategy = routingStrategy
		self.historicalNodeCount = historicalNodeCount
		self.queryList = list()
		self.historicalNodeList = self.createHistoricalNodes(self.historicalNodeCount)

	def createHistoricalNodes(self, historicalNodeCount):
		historicalnodelist = list()
		for i in xrange(historicalNodeCount):
			historicalnodelist.append(HistoricalNode(i+1))
		return historicalnodelist

	def log(self, message):
		print "%s,%s: %s" % (self.placementStrategy, self.routingStrategy, message)

	def printStatistics(self):
    		replicalist = set()
    		numreplicas = 0
    		maxtime = 0
    		for node in self.historicalNodeList:
    		    self.log("Compute Ends for %d at %d" % (node.getID(), node.computeEndsAt()))
    		    if node.computeEndsAt() >= maxtime:
    		        maxtime = node.computeEndsAt()

    		    nodereplicamap = node.getReplicaCount()
    		    numreplicas += sum(nodereplicamap.values())
    		    for (key, value) in nodereplicamap.items():
    		        replicalist.add(key)

    		self.log("Average Replication Factor: %f" % (float(numreplicas) / len(replicalist)))
   		self.log("Overall Completion Time: %d" % maxtime)

        def findRoutableQueries(self, candidateList, historicalNodeList):
            routinglist = list()
            querylist = list()
        
            loadedsegments = list()
            for node in historicalNodeList:
                loadedsegments.extend(node.getReplicaCount().keys())
        
            uniquesegments = set(loadedsegments)
        
            for candidate in candidateList:
                placed = True
                for segment in candidate.segmentList:
                    if segment not in uniquesegments:
                        placed = False
                        break
                if placed == False:
                    querylist.append(candidate)
                else:
                    routinglist.append(candidate)
        
            return (routinglist, querylist)

	def routeQueries(self, newList, segmentRunningCount, time):
		self.queryList.extend(newList)

    		self.log("Routing Queries")
    		(routinglist, self.queryList) = self.findRoutableQueries(self.queryList, self.historicalNodeList)
    		if len(routinglist) > 0:
    		    Utils.printQueryList(routinglist)
    		    Broker.routeQueries(routinglist, self.historicalNodeList, self.routingStrategy, segmentRunningCount, time)
		    Utils.printQueryAssignment(self.historicalNodeList)
    		    #self.log("Overall Completion Time: %d" % timetaken)

        def placeSegments(self, segmentList, deepStorage, percentReplicate, replicationFactor):
		self.log("Placing Segments")
		Coordinator.placeSegmentsAndReplicas(segmentList, deepStorage, percentReplicate, replicationFactor, self.historicalNodeList, self.queryList, self.placementStrategy)
		Utils.printSegmentPlacement(self.historicalNodeList)
		#self.log("Average Replication: %f" % avgreplication)
