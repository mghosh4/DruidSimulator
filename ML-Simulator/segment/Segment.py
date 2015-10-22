class Segment(object):
	def __init__(self, source, time, value):
		self.source = source
		self.time = time
		self.value = value
		
	def info(self):
		return str(self.time)

	def getTime(self):
		return self.time
