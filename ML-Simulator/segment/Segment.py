class Segment(object):
	def __init__(self, source, time, segid):
	    self.source = source
	    self.time = time
	    self.segid = segid
	    self.size = 1
		
	def info(self):
            return str(self.time) + ":" + str(self.segid)

	def getTime(self):
	    return self.time

	def getSegmentIdentifier(self):
	    return self.segid
