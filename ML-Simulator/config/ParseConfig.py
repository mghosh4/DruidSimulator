import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.segmentcount = 10
		self.querycount = 5
		self.segmentperquery = 3
		self.distribution = "uniform"
		self.historicalnodecount = 3
		self.placestrategy = "random"

		self.parseConfigFile(configFilePath)

	def parseConfigFile(self, configFilePath):
		with open(configFilePath) as f:
			for line in f:
				if line[0] == "#":
					continue
				
				words = line.split("=")
				key = words[0]
				value = words[1].replace("\n", "")
				
				if key == "segmentcount":
					self.segmentcount = int(value)
				elif key == "querycount":
					self.querycount = int(value)
				elif key == "segmentperquery":
					self.segmentperquery = int(value)
				elif key == "distribution":
					self.distribution = value
				elif key == "historicalnodecount":
					self.historicalnodecount = int(value)
				elif key == "placementstrategy":
					self.placementstrategy = value

	def getSegmentCount(self):
		return self.segmentcount

	def getQueryCount(self):
		return self.querycount

	def getSegmentPerQuery(self):
		return self.segmentperquery

	def getDistribution(self):
		return self.distribution

	def getHistoricalNodeCount(self):
		return self.historicalnodecount

	def getPlacementStrategy(self):
		return self.placementstrategy

	def printConfig(self):
		print "Config details"
		print "Segment Count : %d" % self.getSegmentCount()
		print "Query Count : %d" % self.getQueryCount()
		print "Segment Per Query : %d" % self.getSegmentPerQuery()
		print "Distribution : " + self.getDistribution()
		print "Historical Node Count : %d" % self.getHistoricalNodeCount()
		print "Placement Strategy : " + self.getPlacementStrategy()
		
		
