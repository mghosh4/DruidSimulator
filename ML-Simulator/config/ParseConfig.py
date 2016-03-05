#TODO: Check if the minsize and maxsize is less than the segmentcount
import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.segmentcount = 30
		self.querycount = 200
		self.qsegdistrib = "latest"
		self.qsizedistrib = "zipfian"
		self.queryminsize = 3
		self.querymaxsize = 20
		self.historicalnodecount = 3
		self.changesegmentdistribution = "false"
		self.burstyquery = "false"
		self.burstyquerymultiplier = 5
		self.burstyqueryinterval = 300
		self.burstysegment = "false"
		self.burstysegmentmultiplier = 50
		self.burstysegmentinterval = 300
		self.hottierthreshold = 300
		self.coldtierthreshold = 800
		self.adaptivehistorycount = 5
		self.bestfithistorycount = 5

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
				elif key == "changesegmentdistribution":
					self.changesegmentdistribution = value
				elif key == "burstyquery":
					self.burstyquery = value
				elif key == "burstyquerymultiplier":
					self.burstyquerymultiplier = int(value)
				elif key == "burstyqueryinterval":
					self.burstyqueryinterval = int(value)
				elif key == "burstysegment":
					self.burstysegment = value
				elif key == "burstysegmentmultiplier":
					self.burstysegmentmultiplier = int(value)
				elif key == "burstysegmentinterval":
					self.burstysegmentinterval = int(value)
				elif key == "hottierthreshold":
					self.hottierthreshold = int(value)
				elif key == "coldtierthreshold":
					self.coldtierthreshold = int(value)
				elif key == "adaptivehistorycount":
					self.adaptivehistorycount = int(value)
				elif key == "bestfithistorycount":
					self.bestfithistorycount = int(value)

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

	def getChangeSegmentDistribution(self):
		return self.changesegmentdistribution == "true"

	def getBurstyQuery(self):
		return self.burstyquery == "true"

	def getBurstyQueryMultiplier(self):
		return self.burstyquerymultiplier

	def getBurstyQueryInterval(self):
		return self.burstyqueryinterval

	def getBurstySegment(self):
		return self.burstysegment == "true"

	def getBurstySegmentMultiplier(self):
		return self.burstysegmentmultiplier

	def getBurstySegmentInterval(self):
		return self.burstysegmentinterval

	def getHotTierThreshold(self):
		return self.hottierthreshold

	def getColdTierThreshold(self):
		return self.coldtierthreshold

	def getAdaptiveHistoryCount(self):
		return self.adaptivehistorycount

	def getBestFitHistoryCount(self):
		return self.bestfithistorycount


	def printConfig(self):
		print "Config details"
		print "Segment Count : %d" % self.getSegmentCount()
		print "Query Count : %d" % self.getQueryCount()
		print "Query Segment Distribution : " + self.getQuerySegmentDistribution()
		print "Query Size Distribution : " + self.getQuerySizeDistribution()
		print "Minimum Query Size : %d" % self.getQueryMinSize()
		print "Maximum Query Size : %d" % self.getQueryMaxSize()
		print "Historical Node Count : %d" % self.getHistoricalNodeCount()
		print "Change Segment Distribution : " + self.changesegmentdistribution
		print "Bursty Query : " + self.burstyquery
		print "Bursty Query Multiplier : %d" % self.burstyquerymultiplier
		print "Bursty Query Interval : %d" % self.burstyqueryinterval
		print "Bursty Segment : " + self.burstysegment
		print "Bursty Segment Multiplier : %d" % self.burstysegmentmultiplier
		print "Bursty Segment Interval : %d" % self.burstysegmentinterval
