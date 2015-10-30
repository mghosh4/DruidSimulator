from Node import Node

class HistoricalNode(Node):
	def __init__(self, id):
		self.segmentlist = list()
		self.replicalist = list()
		self.id = id

	def getID(self):
	        return self.id
			
	def add_segment(self, segment):
		self.segmentlist.append(segment)
		
	def add_replica(self, segment):
		self.replicalist.append(replica)
	
	def queue_size(self):
		return len(self.segmentlist)
	
	def replica_size(self):
		return len(self.replicalist)	
		
	def lookup(self, time):
		for segment in self.segmentlist:
			if segment.time == time:
				return True
	
		return False

	def calculateDruidCost(self):
	    	cost = 0
	    	for segment in self.segmentlist:
	    	    cost += segment.getTime()
	
	    	return cost

	def printSegmentList(self):
		print "Historical Node %d" % self.id
		print ', '.join(x.info() for x in self.segmentlist)	
		
