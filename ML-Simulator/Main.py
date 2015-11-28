import os,sys
import time
import threading
sys.path.append(os.path.abspath('query'))
sys.path.append(os.path.abspath('distribution'))
sys.path.append(os.path.abspath('config'))
sys.path.append(os.path.abspath('node'))
sys.path.append(os.path.abspath('utilities'))
sys.path.append(os.path.abspath('replicationstrategy'))

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

	configfile = getConfigFile(args)
	return configfile

def getConfig(configFile):
	configfilePath = configFile
	return ParseConfig(configfilePath)
	
def createHistoricalNodes(historicalNodeCount):
	historicalnodelist = list()
	for i in xrange(historicalNodeCount):
		historicalnodelist.append(HistoricalNode(i+1))
	return historicalnodelist

def printQueryList(queryList):
	for query in queryList:
		print query.info()

def runExperiment(historicalNodeCount, segmentList, percentreplicate, replicationFactor, queryList, placementStrategy, routingStrategy):
	segmentcount = len(segmentList)

	#Creating Historical Nodes
	print "Creating Historical Nodes"
	historicalnodelist = createHistoricalNodes(historicalNodeCount)
	
	#Placing Segments
	print "Placing Segments"
	avgreplication = Coordinator.placeSegmentsAndReplicas(segmentList, percentreplicate, replicationFactor, historicalnodelist, queryList, placementStrategy)
	Coordinator.printCurrentPlacement(historicalnodelist)
	print("%s,%s,%f Average Replication: %f" % (placementStrategy, routingStrategy, percentreplicate, avgreplication))
	
	#Calculating Scores
	print "Routing Queries"
	timetaken = Broker.routeQueries(queryList, historicalnodelist, routingStrategy, segmentcount)
	print("%s,%s,%f Overall Completion Time: %d" % (placementStrategy, routingStrategy, percentreplicate, timetaken))

#def runExperimentThread(threads, historicalNodeCount, segmentList, replicationFactor, queryList, placementStrategy, routingStrategy):
#        thread = threading.Thread(target=runExperiment, args=(historicalnodecount, segmentlist, replicationfactor, querylist, placementstrategy, routingstrategy,))
#        threads.append(thread)
#        thread.start()

configfile = checkAndReturnArgs(sys.argv)
config = getConfig(configfile)

config.printConfig()

segmentcount = config.getSegmentCount()
querycount = config.getQueryCount()
querysegmentdistribution = config.getQuerySegmentDistribution()
querysizedistribution = config.getQuerySizeDistribution()
queryminsize = config.getQueryMinSize()
querymaxsize = config.getQueryMaxSize()
historicalnodecount = config.getHistoricalNodeCount()
placementstrategy = config.getPlacementStrategy()
replicationfactor = config.getReplicationFactor()
percentreplicate = config.getPercentReplicate()

#Generating Segments indexed starting from 1
print "Generating Segments"
segmentlist = RealTimeNode.generateSegments(segmentcount)
RealTimeNode.printlist(segmentlist)

#Generating Queries
print "Generating Queries"
querylist = QueryGenerator.generateQueries(querycount, segmentcount, DistributionFactory.createSegmentDistribution(querysegmentdistribution), queryminsize, querymaxsize, DistributionFactory.createSizeDistribution(querysizedistribution));
printQueryList(querylist)

###  DRUID COST BASED
placementstrategy = "druidcostbased"

for replicationfactor in xrange(2, 3):
	### Random Routing Stretagy
	routingstrategy = "random"
	runExperiment(historicalnodecount, segmentlist, percentreplicate, replicationfactor, querylist, placementstrategy, routingstrategy)

	### Connection Count Based Strategy
	routingstrategy = "chooseleastloaded"
	runExperiment(historicalnodecount, segmentlist, percentreplicate, replicationfactor, querylist, placementstrategy, routingstrategy)

###  RANDOM BALL BASED
placementstrategy = "randomballbased"
routingstrategy = "randomballbased"
runExperiment(historicalnodecount, segmentlist, percentreplicate, replicationfactor, querylist, placementstrategy, routingstrategy)

###  BEST FIT
placementstrategy = "bestfit"
routingstrategy = "bestfit"
runExperiment(historicalnodecount, segmentlist, percentreplicate, replicationfactor, querylist, placementstrategy, routingstrategy)
