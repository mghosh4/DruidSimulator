from Distribution import *
class DistributionFactory(object):

	@staticmethod
	def createDistribution(distribution):
		if distribution == "uniform":
			return Uniform()
		elif distribution == "zipfian":
			return Zipfian()
		elif distribution == "latest":
			return Latest()
