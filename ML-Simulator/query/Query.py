from collections import Counter

class Query(object):
	def __init__(self, index):
		self.index = index
		self.segmentTimeList = Counter()
		
	def show(self):
		print "Query %d: %s" % (self.index, ', '.join(str(x) for x in self.segmentTimeList.iterkeys()))

	def getID(self):
		return self.index

	def add(self, segment):
		self.segmentTimeList[segment] += 1

	def getSegmentTimeCount(self):
		return self.segmentTimeList
