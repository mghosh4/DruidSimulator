import sys,os
sys.path.append(os.path.abspath('segment'))

from Node import Node
from SegmentGenerator import SegmentGenerator

class RealTimeNode(Node):
	segmentRunningCount = 0

	@staticmethod
	def generateSegments(segmentSize):
		segmentlist = SegmentGenerator.populate(RealTimeNode.segmentRunningCount, segmentSize)
		RealTimeNode.segmentRunningCount += segmentSize
		return segmentlist

	@staticmethod
	def printlist(segmentlist):
		print ', '.join(x.info() for x in segmentlist)	
