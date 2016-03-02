from collections import Counter

class Query(object):
	def __init__(self, index, time):
		self.index = index
                self.startTime = time
		self.segmentList = Counter()
                self.endTime = -1
		
	def show(self):
		print "Query %d: %s" % (self.index, ', '.join(x.info() for x in self.segmentList.iterkeys()))

	def getID(self):
		return self.index

	def add(self, segment):
		self.segmentList[segment] += 1

	def getSegmentList(self):
		return self.segmentList
		
	def getSegmentCount(self):
		return len(self.segmentList)

        def setEndTime(self, time):
            self.endTime = time

        def setStartTime(self, time):
            self.startTime = time

        def getCompletionTime(self):
            assert self.endTime >= 0
            return self.endTime - self.startTime
