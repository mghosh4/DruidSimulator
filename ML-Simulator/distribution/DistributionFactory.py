from Distribution import *
class DistributionFactory(object):

	@staticmethod
	def createSegmentDistribution(distribution):
		if distribution == "uniform":
			return Uniform()
		elif distribution == "zipfian":
			return ScrambledZipfian()
		elif distribution == "latest":
			return Latest()
	
	@staticmethod
	def createSizeDistribution(distribution):
		if distribution == "uniform":
			return Uniform()
		elif distribution == "zipfian":
			return Zipfian()
