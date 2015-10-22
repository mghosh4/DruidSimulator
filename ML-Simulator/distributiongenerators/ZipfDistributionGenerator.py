import numpy
from DistributionGenerator import DistributionGenerator
class ZipfDistributionGenerator(DistributionGenerator):

	def generateQuerySegments(self, numSegments, segmentCount):
		shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
		zipfsegment = list()

		while len(zipfsegment) < numSegments:
			zipfsegment = numpy.random.zipf(shape, 3*numSegments)
			zipfsegment = zipfsegment[zipfsegment<=segmentCount]
			print "Zipfian Count: %d" % len(zipfsegment)	

		zipfsegment = zipfsegment[0:numSegments]
		return [ int(x) for x in zipfsegment ]
