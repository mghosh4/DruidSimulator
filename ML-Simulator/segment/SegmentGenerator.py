from Segment import Segment

class SegmentGenerator(object):

	@staticmethod
	def populate(time, count, size):
		segmentlist = list()
		for i in xrange(count, count + size):
			segmentlist.append(Segment(0, time, i)) # need to modify
		return segmentlist
		
	@staticmethod
	def printAll(self, segmentlist):
	    for data in segmentlist:
	    	data.show()
