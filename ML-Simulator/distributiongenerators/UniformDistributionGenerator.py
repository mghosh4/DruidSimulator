import numpy
from DistributionGenerator import DistributionGenerator
class UniformDistributionGenerator(DistributionGenerator):

	def generateQuerySegments(self, numSegments, segmentCount):
		start = 1
		end = segmentCount 

		# start and end is inclusive
		return numpy.random.random_integers(start, end, numSegments)

