class Query(object):
	def __init__(self):
		self.segmentList = list()
		
	def info(self):
		return ', '.join(str(x) for x in self.segmentList)

	def add(self, segment):
		self.segmentList.append(segment)

	def getSegmentCount(self):
		segmentcountmap = dict()
		for segment in self.segmentList:
                    if segment not in segmentcountmap:
                        segmentcountmap[segment] = 0
		    segmentcountmap[segment] += 1

		return segmentcountmap
