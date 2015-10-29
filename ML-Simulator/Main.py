import os,sys
import time
sys.path.append(os.path.abspath('query'))
sys.path.append(os.path.abspath('distribution'))
sys.path.append(os.path.abspath('config'))
sys.path.append(os.path.abspath('node'))
sys.path.append(os.path.abspath('utilities'))

from ParseConfig import ParseConfig
from QueryGenerator import QueryGenerator
from DistributionFactory import DistributionFactory
from HistoricalNode import HistoricalNode
from RealTimeNode import RealTimeNode
from Coordinator import Coordinator
from Broker import Broker


def getConfigFile(args):
	return args[1]

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usage: python " + args[0] + " <config_file>"
		exit()

	configFile = getConfigFile(args)
	return configFile

def getConfig(configFile):
	configFilePath = configFile
	return ParseConfig(configFilePath)
	
def createHistoricalNodes(historicalNodeCount):
	historicalNodeList = list()
	for i in xrange(historicalNodeCount):
		historicalNodeList.append(HistoricalNode(i+1))
	return historicalNodeList

def printQueryList(querylist):
	for query in querylist:
		print query.info()

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

config.printConfig()

segmentCount = config.getSegmentCount()
queryCount = config.getQueryCount()
querysegmentdistribution = config.getQuerySegmentDistribution()
querysizedistribution = config.getQuerySizeDistribution()
queryminsize = config.getQueryMinSize()
querymaxsize = config.getQueryMaxSize()
historicalNodeCount = config.getHistoricalNodeCount()
placementstrategy = config.getPlacementStrategy()

#Creating Historical Nodes
print "Creating Historical Nodes"
historicalNodeList = createHistoricalNodes(historicalNodeCount)
print len(historicalNodeList)

#Generating Segments indexed starting from 1
print "Generating Segments"
segmentList = RealTimeNode.generateSegments(segmentCount)
RealTimeNode.printlist(segmentList)

#Placing Segments
print "Placing Segments"
Coordinator.placeSegments(segmentList, historicalNodeList, placementstrategy)
Coordinator.printCurrentPlacement(historicalNodeList)

#Generating Queries
print "Generating Queries"
querylist = QueryGenerator.generateQueries(queryCount, segmentCount, DistributionFactory.createDistribution(querysegmentdistribution), queryminsize, querymaxsize, DistributionFactory.createDistribution(querysizedistribution));
printQueryList(querylist)

#Calculating Scores
print "Calculating Scores"
Broker.timeCalculation(historicalNodeList, querylist)
