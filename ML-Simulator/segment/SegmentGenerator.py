#derived and modified based on Simulation_Wenting/DataBlock.py 
from Segment import Segment

class SegmentGenerator(object):
	
		
	def populate(self, distribution,size):
		list = []
		for i in range(size):
			self.list.append(Segment( 0, i+1, i)) # need to modify
		return list
		
		
	def printAll(self):
	    for data in self.list:
	    	data.show()
		
