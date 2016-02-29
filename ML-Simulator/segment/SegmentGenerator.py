from Segment import Segment

class SegmentGenerator(object):

	@staticmethod
	def populate(start, size):
		segmentlist = list()
		for i in xrange(start, start + size):
			segmentlist.append(Segment(0, i, i)) # need to modify
		return segmentlist
		
	@staticmethod
	def printAll(self, segmentlist):
	    for data in segmentlist:
	    	data.show()
