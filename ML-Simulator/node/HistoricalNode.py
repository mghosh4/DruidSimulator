from collections import Counter
from Node import Node

class HistoricalNode(Node):
	def __init__(self, id):
		self.segmentCount = Counter()
		self.querytime = list()
		self.computeEndTime = 0
		self.id = id

	def getID(self):
	        return self.id
			
	def addSegment(self, segment):
		self.segmentCount[segment] += 1
		
	def removeSegment(self, segment):
		if self.segmentCount[segment] > 0:
			self.segmentCount[segment] -= 1

	def numReplicasPlaced(self):
		return sum(self.segmentCount.values())
	
	def lookupBySegment(self, segment):
		return self.segmentCount[segment] > 0

	def lookupByTime(self, time):
		for segment in self.segmentCount.iterkeys():
			if time == segment.getTime():
				return self.lookupBySegment(segment)

		return False

	def getSegmentAndReplicaList(self):
	    	allsegmentlist = list()
		for key, value in self.segmentCount.iteritems():
		    for _ in xrange(value):
			allsegmentlist.append(key)
	    	return allsegmentlist

	def getSegmentCounts(self):
		return self.segmentCount

	def printSegmentList(self):
		print "Historical Node %d" % self.id
		print ', '.join(x.info() for x in self.getSegmentAndReplicaList())	
	
	def printQueryList(self):
		print "Historical Node %d" % self.id
		print self.querytime
	
	def routeQuery(self, query, time):
		if time <= self.computeEndTime:
			self.computeEndTime += 1
		else:
			self.computeEndTime = time
		self.querytime.append((self.computeEndTime, query.getID()))

	def computeEndsAt(self):
		return self.computeEndTime
