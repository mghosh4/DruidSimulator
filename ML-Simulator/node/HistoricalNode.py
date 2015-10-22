from Node import Node

class HistoricalNode(Node):
	def __init__(self, id):
		self.segmentlist = list()
		self.id = id

    def getID(self):
        return self.id
		
	def add_segment(self, segment):
		self.segmentlist.append(segment)

	def queue_size(self):
		return len(self.segmentlist)
		
	def lookup(self, value):
		for segment in self.segmentlist:
			if segment.value == value:
				return True
		return False

    def calculateDruidCost(self):
        cost = 0
        for segment in self.segmentlist:
            cost += segment.getID()

        return cost