#TODO: Check if the minsize and maxsize is less than the segmentcount
import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.segmentcount = 10
		self.querycount = 5
		self.qsegdistrib = "uniform"
		self.qsizedistrib = "uniform"
		self.queryminsize = 3
		self.querymaxsize = 7
		self.historicalnodecount = 3
		self.placementstrategy = "random"
		self.replicationfactor = 3
		self.percentreplicate = 0.3

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
				elif key == "querysegmentdistribution":
					self.qsegdistrib = value
				elif key == "querysizedistribution":
					self.qsizedistrib = value
				elif key == "queryminsize":
					self.queryminsize = int(value)
				elif key == "querymaxsize":
					self.querymaxsize = int(value)
				elif key == "historicalnodecount":
					self.historicalnodecount = int(value)
				elif key == "placementstrategy":
					self.placementstrategy = value
				elif key == "replicationfactor":
					self.replicationfactor = int(value)
				elif key == "percentreplicate":
					self.percentreplicate = float(value)

	def getSegmentCount(self):
		return self.segmentcount

	def getQueryCount(self):
		return self.querycount

	def getQuerySegmentDistribution(self):
		return self.qsegdistrib

	def getQuerySizeDistribution(self):
		return self.qsizedistrib

	def getQueryMinSize(self):
		return self.queryminsize

	def getQueryMaxSize(self):
		return self.querymaxsize

	def getHistoricalNodeCount(self):
		return self.historicalnodecount

	def getPlacementStrategy(self):
		return self.placementstrategy

	def getReplicationFactor(self):
		return self.replicationfactor

	def getPercentReplicate(self):
		return self.percentreplicate

	def printConfig(self):
		print "Config details"
		print "Segment Count : %d" % self.getSegmentCount()
		print "Query Count : %d" % self.getQueryCount()
		print "Query Segment Distribution : " + self.getQuerySegmentDistribution()
		print "Query Size Distribution : " + self.getQuerySizeDistribution()
		print "Minimum Query Size : %d" % self.getQueryMinSize()
		print "Maximum Query Size : %d" % self.getQueryMaxSize()
		print "Historical Node Count : %d" % self.getHistoricalNodeCount()
		print "Placement Strategy : " + self.getPlacementStrategy()
		print "Replication Factor : %d" % self.getReplicationFactor()
		print "Percent Replicate : %f" % self.getPercentReplicate()
