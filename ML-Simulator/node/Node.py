import random
import utilities/Global as g

class Node(object):
	def __init__(self, type):
		self.nodetype = type
	
	def show(self):
		print "Node type: ", self.nodetype
	
	
