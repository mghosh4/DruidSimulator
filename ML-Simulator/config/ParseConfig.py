#TODO: Check if the minsize and maxsize is less than the segmentcount
import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.segmentcount = 30
		self.preloadsegment = 10
		self.querycount = 200
		self.qsegdistrib = "latest"
		self.qsizedistrib = "zipfian"
		self.queryminsize = 3
		self.querymaxsize = 20
		self.queryperinterval = 10
		self.historicalnodecount = 3
		self.placementstrategy = "druidcostbased"
		self.routingstrategy = "chooseleastloaded"
		self.replicationfactor = 3
		self.percentreplicate = 0.3
		self.changesegmentdistribution = "false"
		self.burstyquery = "false"
		self.burstyquerymultiplier = 5
		self.burstyqueryinterval = 3

		if configFilePath is not  None:
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
				if key == "preloadsegment":
					self.preloadsegment = int(value)
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
				elif key == "queryperinterval":
					self.queryperinterval = int(value)
				elif key == "historicalnodecount":
					self.historicalnodecount = int(value)
				elif key == "placementstrategy":
					self.placementstrategy = value
				elif key == "routingstrategy":
					self.routingstrategy = value
				elif key == "replicationfactor":
					self.replicationfactor = int(value)
				elif key == "percentreplicate":
					self.percentreplicate = float(value)
				elif key == "changesegmentdistribution":
					self.changesegmentdistribution = value
				elif key == "burstyquery":
					self.burstyquery = value
				elif key == "burstyquerymultiplier":
					self.burstyquerymultiplier = int(value)
				elif key == "burstyqueryinterval":
					self.burstyqueryinterval = int(value)

	def getSegmentCount(self):
		return self.segmentcount

	def getPreLoadSegment(self):
		return self.preloadsegment

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

	def getQueryPerInterval(self):
		return self.queryperinterval

	def getHistoricalNodeCount(self):
		return self.historicalnodecount

	def getPlacementStrategy(self):
		return self.placementstrategy

	def getRoutingStrategy(self):
		return self.routingstrategy

	def getReplicationFactor(self):
		return self.replicationfactor

	def getPercentReplicate(self):
		return self.percentreplicate

	def getChangeSegmentDistribution(self):
		return self.changesegmentdistribution

	def getBurstyQuery(self):
		return self.burstyquery

	def getBurstyQueryMultiplier(self):
		return self.burstyquerymultiplier

	def getBurstyQueryInterval(self):
		return self.burstyqueryinterval

	def printConfig(self):
		print "Config details"
		print "Segment Count : %d" % self.getSegmentCount()
		#print "Pre Load Segment Count : %d" % self.getPreLoadSegment()
		print "Query Count : %d" % self.getQueryCount()
		print "Query Segment Distribution : " + self.getQuerySegmentDistribution()
		print "Query Size Distribution : " + self.getQuerySizeDistribution()
		print "Minimum Query Size : %d" % self.getQueryMinSize()
		print "Maximum Query Size : %d" % self.getQueryMaxSize()
		#print "Query Per Interval : %d" % self.getQueryPerInterval()
		print "Historical Node Count : %d" % self.getHistoricalNodeCount()
		#print "Placement Strategy : " + self.getPlacementStrategy()
		#print "Routing Strategy : " + self.getRoutingStrategy()
		#print "Replication Factor : %d" % self.getReplicationFactor()
		#print "Percent Replicate : %f" % self.getPercentReplicate()
