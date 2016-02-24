from ReplicationStrategy import *

class ReplicationFactory(object):

	@staticmethod
	def createReplicationStrategy(strategy):
		if strategy == "fixed":
			return Fixed()
		elif strategy == "tiered":
			return Tiered()
		elif strategy == "adaptive":
			return Adaptive()
		elif strategy == "bestfit-d":
			return BestFit()
		elif strategy == "bestfit-s":
			return BestFit()
