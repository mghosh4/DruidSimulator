from collections import Counter

class Query(object):
	def __init__(self, index, time):
		self.index = index
                self.startTime = time
		self.segmentTimeList = Counter()
                self.endTime = -1
		
	def show(self):
		print "Query %d: %s" % (self.index, ', '.join(str(x) for x in self.segmentTimeList.iterkeys()))

	def getID(self):
		return self.index

	def add(self, segment):
		self.segmentTimeList[segment] += 1

	def getSegmentTimeCount(self):
		return self.segmentTimeList

        def setEndTime(self, time):
            assert self.endTime == -1
            self.endTime = time

        def setStartTime(self, time):
            self.startTime = time

        def getCompletionTime(self):
            if self.endTime < 0:
                return -1
            return self.endTime - self.startTime
