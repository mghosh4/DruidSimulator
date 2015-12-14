from Node import Node

class HistoricalNode(Node):
	def __init__(self, id):
		self.segmentlist = dict()
		self.replicacount = dict()
		self.querytime = list()
		self.computeEndTime = 0
		self.id = id

	def getID(self):
	        return self.id
			
	def add_segment(self, segment):
		if segment.time not in self.replicacount:
			self.segmentlist[segment.time] = segment
			self.replicacount[segment.time] = 1
		else:
			self.replicacount[segment.time] += 1

		
	def add_replica(self, segment):
		if segment.time not in self.replicacount:
			self.segmentlist[segment.time] = segment
			self.replicacount[segment.time] = 1
		else:
			self.replicacount[segment.time] += 1

	def queue_size(self):
		return sum(self.replicacount.values())
	
	def lookup(self, time):
		if time in self.replicacount:
			return True
	
		return False

	def getSegmentAndReplicaList(self):
	    	allsegmentlist = list()
		for key, value in self.replicacount.iteritems():
			for _ in xrange(value):
				allsegmentlist.append(self.segmentlist[key])
	    	return allsegmentlist

	def getReplicaCount(self):
		return self.replicacount

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
