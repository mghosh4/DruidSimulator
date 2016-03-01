import os,sys
sys.path.append(os.path.abspath('node'))
sys.path.append(os.path.abspath('utilities'))
from collections import Counter
import copy

from HistoricalNode import HistoricalNode
from Coordinator import Coordinator
from Broker import Broker
from Utils import Utils

class Strategy(object):
	def __init__(self, historicalNodeCount, placementStrategy, replicationStrategy, routingStrategy):
		self.placementStrategy = placementStrategy
		self.replicationStrategy = replicationStrategy
		self.routingStrategy = routingStrategy
		self.historicalNodeCount = historicalNodeCount
		self.queryList = list()
		self.historicalNodeList = self.createHistoricalNodes(self.historicalNodeCount)
		self.segmentReplicaCount = Counter()
		self.pastHistory = list()
                self.queriesrouted = 0
                self.numsegmentloads = 0
                self.totalcomputetime = 0
                self.totalcompletiontime = 0

	def createHistoricalNodes(self, historicalNodeCount):
		historicalnodelist = list()
		for i in xrange(historicalNodeCount):
			historicalnodelist.append(HistoricalNode(i+1))
		return historicalnodelist

	def log(self, time, message):
		print "%d, %s: %s" % (time, self.replicationStrategy, message)

	def printStatistics(self, time):
    		replicalist = Counter()
    		numreplicas = 0
    		maxtime = 0
    		for node in self.historicalNodeList:
    		    self.log(time, "Compute Ends for %d at %d" % (node.getID(), node.computeEndsAt()))
    		    if node.computeEndsAt() >= maxtime:
    		        maxtime = node.computeEndsAt()

                    replicalist += node.getSegmentCounts()

                totalreplicas = sum(replicalist.values())

            if querysegmentcount > -1:
                self.log(time, "Overall Segment Throughput: %f" % (float(querysegmentcount) / float(maxtime)))

            self.log(time, "Total Number Replicas: %d" % totalreplicas)
            self.log(time, "Average Replication Factor: %f" % (float(totalreplicas) / len(replicalist)))
            self.log(time, "Number Segment Loads: %d" % self.numsegmentloads)
            self.log(time, "Total Routing Time: %d" % maxtime)
            self.log(time, "Total Running Time: %d" % self.totalcomputetime)

    def findRoutableQueries(self, candidateList, historicalNodeList):
            routinglist = list()
            querylist = list()
        
            loadedsegments = Counter()
            for node in historicalNodeList:
                loadedsegments += node.getSegmentCounts()
        
            uniquetimes = list()
            for segment in loadedsegments.keys():
                uniquetimes.append(segment.getTime())
        
            for candidate in candidateList:
                placed = True
                for time in candidate.segmentTimeList:
                    if time not in uniquetimes:
                        placed = False
                        break
                if placed == False:
                    querylist.append(copy.copy(candidate))
                else:
                    routinglist.append(copy.copy(candidate))
        
            return (routinglist, querylist)

    def routeQueries(self, newList, segmentRunningCount, time):
        self.queryList.extend(newList)

        self.log(time, "Routing Queries")
        (routinglist, self.queryList) = self.findRoutableQueries(self.queryList, self.historicalNodeList)
        if len(routinglist) > 0:
            self.queriesrouted += len(routinglist)
        Utils.printQueryList(routinglist)
        Broker.routeQueries(routinglist, self.historicalNodeList, self.routingStrategy, segmentRunningCount, time)
        Utils.printQueryAssignment(self.historicalNodeList)
                #self.log("Overall Completion Time: %d" % timetaken)

    def allQueriesRouted(self):
            if (len(self.queryList) > 0):
                Utils.printQueryList(self.queryList)
                segmenttimecount = Counter()
            for query in self.queryList:
                segmenttimecount += query.getSegmentTimeCount()

                loadedtimelist = list()
                for segment in self.segmentReplicaCount.iterkeys():
                    loadedtimelist.append(segment.getTime())

                for time in segmenttimecount.iterkeys():
                    if time not in loadedtimelist:
                        print time

            return len(self.queryList) == 0

    def placeSegments(self, segmentList, deepStorage, time):
        self.log(time, "Placing Segments")
        (numloads, computetime) = Coordinator.placeSegmentsAndReplicas(segmentList, deepStorage, self.historicalNodeList, self.queryList, self.placementStrategy, self.replicationStrategy, self.segmentReplicaCount, self.pastHistory, time)
        self.numsegmentloads += numloads
        self.totalcomputetime += computetime
        Utils.printSegmentPlacement(self.historicalNodeList)
        #self.log("Average Replication: %f" % avgreplication)
