#derived and modified based on Simulation_Wenting/DataBlock.py 
from Segment import Segment

class SegmentGenerator(object):
	
	count = 0 
		
	def populate(self, distribution,size):
		list = []
		for i in range(size):
			self.list.append(Segment( 0, i+1, i)) # need to modify
						count = count+1
		return list
		
		
	def printAll(self):
	    for data in self.list:
	    	data.show()
		
