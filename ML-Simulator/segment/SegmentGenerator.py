from Segment import Segment

class SegmentGenerator(object):

	@staticmethod
	def populate(size):
		segmentlist = list()
		for i in xrange(size):
			segmentlist.append(Segment(0, i+1, i)) # need to modify
		return segmentlist
		
	@staticmethod
	def printAll(self, segmentlist):
	    for data in segmentlist:
	    	data.show()
