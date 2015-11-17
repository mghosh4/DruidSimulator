from RoutingStrategy import *

class RoutingFactory(object):

	@staticmethod
	def createRoutingStrategy(strategy):
		if strategy == "random":
			return Random()
		if strategy == "chooseleastloaded":
			return ChooseLeastLoaded()
		elif strategy == "randomballbased":
			return RandomBallBased()
		elif strategy == "bestfit":
			return BestFit()
