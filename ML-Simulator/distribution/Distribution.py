import numpy
class Uniform(object):

	def generateQuerySegments(self, numSegments, segmentCount):
		start = 1
		end = segmentCount 

		# start and end is inclusive
		return numpy.random.random_integers(start, end, numSegments)

class Zipfian(object):

	def generateQuerySegments(self, numSegments, segmentCount):
		shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
		zipfsegment = list()

		while len(zipfsegment) < numSegments:
			zipfsegment = numpy.random.zipf(shape, 3*numSegments)
			zipfsegment = zipfsegment[zipfsegment<=segmentCount]
			print "Zipfian Count: %d" % len(zipfsegment)	

		zipfsegment = zipfsegment[0:numSegments]
		return [ int(x) for x in zipfsegment ]

class Latest(object):

	def generateQuerySegments(self, numSegments, segmentCount):
		shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
		latestsegment = list()

		while len(latestsegment) < numSegments:
			latestsegment = numpy.random.zipf(shape, 3*numSegments)
			latestsegment = latestsegment[latestsegment<=segmentCount]
			print "Latest Count: %d" % len(latestsegment)

		latestsegment = latestsegment[0:numSegments]
		return [ int(segmentCount - x + 1) for x in latestsegment ]
