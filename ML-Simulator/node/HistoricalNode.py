from Node import Node
import utilities/Global as g
from array import *

class HistoricalNode(Node):
	def __init__(self, type, id):
		self.nodetype = g.HISTORICAL
		self.queue = []
		self.id = id
		
	def add_segment(self, segment):
		self.queue.append(segment)
		return self.queue
		
	def queue_size(self):
		return len(self.queue)
		
	def lookup(value):
		for segment in self.queue
			if segment.value = value
				return True
		return False
		
		
		