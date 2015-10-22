import numpy
from DistributionGenerator import DistributionGenerator
class LatestDistributionGenerator(DistributionGenerator):

	def generateQuerySegments(self, numSegments, segmentCount):
		shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
		latestsegment = list()

		while len(latestsegment) < numSegments:
			latestsegment = numpy.random.zipf(shape, 3*numSegments)
			latestsegment = latestsegment[latestsegment<=segmentCount]
			print "Latest Count: %d" % len(latestsegment)

		latestsegment = latestsegment[0:numSegments]
		return [ int(segmentCount - x + 1) for x in latestsegment ]
