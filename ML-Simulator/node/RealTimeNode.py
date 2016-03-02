import sys,os
sys.path.append(os.path.abspath('segment'))

from Node import Node
from SegmentGenerator import SegmentGenerator

class RealTimeNode(Node):
	@staticmethod
	def generateSegments(time, segmentRunningCount, numSegments):
		segmentlist = SegmentGenerator.populate(time, segmentRunningCount, numSegments)
		return segmentlist

	@staticmethod
	def printlist(segmentlist):
		print ', '.join(x.info() for x in segmentlist)	
