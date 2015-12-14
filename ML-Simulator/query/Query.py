class Query(object):
	def __init__(self, index):
		self.index = index
		self.segmentList = list()
		
	def show(self):
		print "Query %d: %s" % (self.index, ', '.join(str(x) for x in self.segmentList))

	def getID(self):
		return self.index

	def add(self, segment):
		self.segmentList.append(segment)

	def getSegmentCount(self):
		segmentcountmap = dict()
		for segment in self.segmentList:
                    if segment not in segmentcountmap:
                        segmentcountmap[segment] = 0
		    segmentcountmap[segment] += 1

		return segmentcountmap
