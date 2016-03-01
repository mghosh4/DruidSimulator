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
querycount = config.getQueryCount()
querysegmentdistribution = config.getQuerySegmentDistribution()
querysizedistribution = config.getQuerySizeDistribution()
queryminsize = config.getQueryMinSize()
querymaxsize = config.getQueryMaxSize()
queryperinterval = config.getQueryPerInterval()
historicalnodecount = config.getHistoricalNodeCount()

######### DYNAMIC SIMULATION #############
print "Dynamic Simulation"

#Creating Historical Nodes
print "Creating Strategy Combinations"
dynamicstrategies = createDynamicStrategyCombinations(historicalnodecount)

deepstorage = list()
segmentlist = list()
querylist = list()
allquerylist = list()
totaltime = segmentcount
segmentsperinterval = 1
coordinatorinterval = 10
segmentrunningcount = 0
warmuptime = coordinatorinterval
residualtime = totaltime - warmuptime
queryperinterval = int(math.ceil(float(querycount) / residualtime))
totalqueries = queryperinterval * residualtime
printstatinterval = int(math.ceil(float(totaltime) / 100))

print("Total Time: %d" % totaltime)
print("Query Per Interval: %d" % queryperinterval)
print("Total Queries: %d" % totalqueries)

#### RUN Phase ####
for time in xrange(1,totaltime+1):
    #Generating Segments indexed starting from 1
    print "Generating Segments and adding to deep storage"
    newsegments = RealTimeNode.generateSegments(segmentrunningcount+1, segmentsperinterval)
    RealTimeNode.printlist(newsegments)
    segmentlist.extend(newsegments)
    deepstorage.extend(newsegments)
    Utils.printSegmentList(deepstorage)
    segmentrunningcount += 1

    if time >= warmuptime:
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

    #Placing Segments
    if time % coordinatorinterval == 0:
        for strategy in dynamicstrategies:
            strategy.placeSegments(segmentlist, deepstorage, time)
        segmentlist = []

        #Print Statistics
        for strategy in dynamicstrategies:
            strategy.printStatistics(time, -1)

for strategy in dynamicstrategies:
    #Placing Segments
    strategy.placeSegments(segmentlist, deepstorage, time)
    
    #Routing Queries
    strategy.routeQueries(list(), segmentrunningcount, totaltime)

for strategy in dynamicstrategies:
    assert strategy.allQueriesRouted()

#get query segment count
querysegmentcount = 0
for query in allquerylist:
    querysegmentcount += query.getSegmentCount()
#Print Statistics
for strategy in dynamicstrategies:
    strategy.printStatistics(time, querysegmentcount)

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
staticstrategy.printStatistics(0, -1)
