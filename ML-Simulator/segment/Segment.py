class Segment(object):
	#derived and modified based on Simulation_Wenting/DataBlock.py 
	def __init__(self, source, time, value):
		self.source = source
		self.time = time
		self.value = value
		
	def info(self)
		print "segment source: ", self.source, "segment time: ", self.time