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
	
def createDynamicStrategyCombinations(historicalNodeCount):
    strategylist = list()
    placementstrategy = 'druidcostbased'
    routingstrategy = 'chooseleastloaded'
    for replicationstrategy in ['fixed', 'tiered', 'adaptive', 'bestfit-d']:
        strategylist.append(Strategy(historicalNodeCount, placementstrategy, replicationstrategy, routingstrategy))

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
replicationfactor = config.getReplicationFactor()

######### DYNAMIC SIMULATION #############
print "Dynamic Simulation"

#Creating Historical Nodes
print "Creating Strategy Combinations"
dynamicstrategies = createDynamicStrategyCombinations(historicalnodecount)

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
            strategy.placeSegments(segmentlist, deepstorage, time)
        segmentlist = []

        #Print Statistics
        for strategy in dynamicstrategies:
            strategy.printStatistics(time)

for strategy in dynamicstrategies:
    #Placing Segments
    strategy.placeSegments(segmentlist, deepstorage, time)
    
    #Routing Queries
    strategy.routeQueries(list(), segmentrunningcount, totaltime)

#Print Statistics
for strategy in dynamicstrategies:
    strategy.printStatistics(time)

######### STATIC SIMULATION #############
print "Static Simulation"

#Creating Historical Nodes
print "Creating Strategy Combinations"
staticstrategy = Strategy(historicalnodecount, 'druidcostbased', 'bestfit-s', 'chooseleastloaded')

#Routing Queries
staticstrategy.routeQueries(allquerylist, segmentrunningcount, 0)

#Placing Segments
staticstrategy.placeSegments(deepstorage, deepstorage, 0)

#Routing Queries
staticstrategy.routeQueries(list(), segmentrunningcount, 0)

#Print Statistics
staticstrategy.printStatistics(0)
