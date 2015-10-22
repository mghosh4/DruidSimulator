import sys,os
sys.path.append(os.path.abspath('segment'))

from Node import Node
from SegmentGenerator import SegmentGenerator

class RealTimeNode(Node):

	@staticmethod
	def generateSegments(segmentsize):
		segmentlist = SegmentGenerator.populate(segmentsize)
		return segmentlist

	@staticmethod
	def printlist(segmentlist):
		print ', '.join(x.info() for x in segmentlist)	
