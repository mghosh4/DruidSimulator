#derived and modified based on Simulation_Wenting/DataBlock.py 
from Segment import Segment

class SegmentGenerator(object):
	def __init__(self,list,distribution):
		self.size = 0
		self.distribution = distribution
		self.list = []
		
	def populate(self, size, distribution):
		self.size = self.size + size
		self.list.append(DataBlock(i, i, i+1, 1)) # need to modify
		
	def printAll(self):
	    for data in self.list:
	    	data.show()
		
