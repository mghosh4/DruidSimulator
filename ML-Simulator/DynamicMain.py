import os,sys
import time
import threading
import math
sys.path.append(os.path.abspath('query'))
sys.path.append(os.path.abspath('distribution'))
sys.path.append(os.path.abspath('config'))
sys.path.append(os.path.abspath('node'))
sys.path.append(os.path.abspath('utilities'))
sys.path.append(os.path.abspath('replicationstrategy'))
sys.path.append(os.path.abspath('strategy'))

from ParseConfig import ParseConfig
from QueryGenerator import QueryGenerator
from DistributionFactory import DistributionFactory
from Strategy import Strategy
from HistoricalNode import HistoricalNode
from RealTimeNode import RealTimeNode
from Coordinator import Coordinator
from Broker import Broker
from Utils import Utils

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usual Usage: python " + args[0] + " <config_file>"
                return None
	return args[1]

def getConfig(configFile):
	configfilePath = configFile
	return ParseConfig(configfilePath)
	
def createStrategyCombinations(historicalNodeCount):
    strategylist = list()
    for placementstrategy in ['druidcostbased', 'bestfit']:
        for routingstrategy in ['chooseleastloaded']:
            strategylist.append(Strategy(historicalNodeCount, placementstrategy, routingstrategy))

    return strategylist

configfile = checkAndReturnArgs(sys.argv)
config = getConfig(configfile)

config.printConfig()

segmentcount = config.getSegmentCount()
preloadsegment = config.getPreLoadSegment()
querycount = config.getQueryCount()
querysegmentdistribution = config.getQuerySegmentDistribution()
querysizedistribution = config.getQuerySizeDistribution()
queryminsize = config.getQueryMinSize()
querymaxsize = config.getQueryMaxSize()
queryperinterval = config.getQueryPerInterval()
historicalnodecount = config.getHistoricalNodeCount()
placementstrategy = config.getPlacementStrategy()
routingstrategy = config.getRoutingStrategy()
replicationfactor = config.getReplicationFactor()
percentreplicate = config.getPercentReplicate()

######### DYNAMIC SIMULATION #############
print "Dynamic Simulation"

#Creating Historical Nodes
print "Creating Strategy Combinations"
dynamicstrategies = createStrategyCombinations(historicalnodecount)

deepstorage = list()
segmentlist = list()
querylist = list()
allquerylist = list()
totaltime = querycount / queryperinterval
segmentcountinrunphase = segmentcount - preloadsegment
segmentinterval = math.floor(totaltime / segmentcountinrunphase)
segmentsperinterval = 1
if segmentinterval == 0:
    segmentinterval = 1
    segmentsperinterval = int(math.floor(segmentcountinrunphase / totaltime))
coordinatorinterval = 5 * segmentinterval
preloadsegment += totaltime % segmentcountinrunphase

print("Total Time: %d" % totaltime)
print("Pre Load Segment Count: %d" % preloadsegment)
print("Segment Count in Run Phase: %d" % segmentcountinrunphase)
print("Segment Interval: %d" % segmentinterval)
print("Segments per Interval: %s" % segmentsperinterval)

#### LOAD Phase ####
print "Pre loading segments and adding to deep storage"
segmentlist = RealTimeNode.generateSegments(preloadsegment)
deepstorage.extend(segmentlist)
segmentrunningcount = len(deepstorage)
RealTimeNode.printlist(segmentlist)

#### RUN Phase ####
for time in xrange(totaltime):
    #Generating Queries
    print "Generating Queries"
    maxquerysize = min(segmentrunningcount, querymaxsize)
    minquerysize = min(queryminsize, maxquerysize)
    newquerylist = QueryGenerator.generateQueries(queryperinterval, segmentrunningcount, DistributionFactory.createSegmentDistribution(querysegmentdistribution), minquerysize, maxquerysize, DistributionFactory.createSizeDistribution(querysizedistribution));
    Utils.printQueryList(newquerylist)
    allquerylist.extend(newquerylist)

    #Routing Queries
    for strategy in dynamicstrategies:
        strategy.routeQueries(newquerylist, segmentrunningcount, time)

    if time % segmentinterval == 0:
        #Generating Segments indexed starting from 1
        print "Generating Segments and adding to deep storage"
        newsegments = RealTimeNode.generateSegments(segmentsperinterval)
        RealTimeNode.printlist(newsegments)
        segmentlist.extend(newsegments)
        deepstorage.extend(newsegments)
        segmentrunningcount = len(deepstorage)

    #Placing Segments
    if time % coordinatorinterval == 0:
        for strategy in dynamicstrategies:
            strategy.placeSegments(segmentlist, deepstorage, percentreplicate, replicationfactor)
        segmentlist = []

for strategy in dynamicstrategies:
    #Placing Segments
    strategy.placeSegments(segmentlist, deepstorage, percentreplicate, replicationfactor)
    
    #Routing Queries
    strategy.routeQueries(list(), segmentrunningcount, totaltime)

#Print Statistics
for strategy in dynamicstrategies:
    strategy.printStatistics()

######### STATIC SIMULATION #############
print "Static Simulation"

#Creating Historical Nodes
print "Creating Strategy Combinations"
staticstrategies = createStrategyCombinations(historicalnodecount)

for strategy in staticstrategies:
    #Routing Queries
    strategy.routeQueries(allquerylist, segmentrunningcount, 0)

    #Placing Segments
    strategy.placeSegments(deepstorage, deepstorage, percentreplicate, replicationfactor)

    #Routing Queries
    strategy.routeQueries(list(), segmentrunningcount, 0)

#Print Statistics
for strategy in staticstrategies:
    strategy.printStatistics()
