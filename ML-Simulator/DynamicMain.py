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
    #for replicationstrategy in ['adaptive']:
        strategylist.append(Strategy(historicalNodeCount, placementstrategy, replicationstrategy, routingstrategy))

    return strategylist

configfile = checkAndReturnArgs(sys.argv)
config = getConfig(configfile)

config.printConfig()

segmentcount = config.getSegmentCount()
querycount = config.getQueryCount()
historicalnodecount = config.getHistoricalNodeCount()
segmentsperinterval = 1
coordinatorinterval = 10

querysegmentdistribution = config.getQuerySegmentDistribution()
querysizedistribution = config.getQuerySizeDistribution()
queryminsize = config.getQueryMinSize()
querymaxsize = config.getQueryMaxSize()

changequerydistribution = config.getChangeSegmentDistribution()
newquerysegmentdistribution = "uniform"

burstyquery = config.getBurstyQuery()
burstyquerymultiplier = config.getBurstyQueryMultiplier()
burstyqueryinterval = config.getBurstyQueryInterval()

burstysegment = config.getBurstySegment()
burstysegmentmultiplier = config.getBurstySegmentMultiplier()
burstysegmentinterval = config.getBurstySegmentInterval()

######### DYNAMIC SIMULATION #############
print "Dynamic Simulation"

#Creating Historical Nodes
print "Creating Strategy Combinations"
dynamicstrategies = createDynamicStrategyCombinations(historicalnodecount)

deepstorage = dict()
segmentlist = list()
querylist = list()
allquerylist = list()
segmentrunningcount = 0

totaltime = segmentcount / segmentsperinterval
warmuptime = coordinatorinterval
residualtime = totaltime - warmuptime
queryperinterval = int(math.ceil(float(querycount) / residualtime))
totalqueries = queryperinterval * residualtime
printstatinterval = int(math.ceil(float(totaltime) / 100))
changedistributionat = totaltime/2

print("Total Time: %d" % totaltime)
print("Query Per Interval: %d" % queryperinterval)
print("Total Queries: %d" % totalqueries)

#### RUN Phase ####
for time in xrange(1,totaltime+1):

    #Generating Segments indexed starting from 1
    print "Generating Segments and adding to deep storage"
    numsegments = segmentsperinterval
    if burstysegment == True and time % burstysegmentinterval == 0:
        print "Segment Burst"
        numsegments *= burstysegmentmultiplier

    newsegments = RealTimeNode.generateSegments(time, segmentrunningcount+1, numsegments)
    RealTimeNode.printlist(newsegments)
    segmentlist.extend(newsegments)
    deepstorage[time] = newsegments
    segmentrunningcount += numsegments

    if time >= warmuptime:
        #Generating Queries
        print "Generating Queries"
        if changequerydistribution == True and time == changedistributionat:
            print "Distribution Change"
            querysegmentdistribution=newquerysegmentdistribution
        
        maxqueryperiod = min(time, querymaxsize)
        minqueryperiod = min(queryminsize, maxqueryperiod)

        numqueries = queryperinterval;
        if burstyquery == True and time % burstyqueryinterval == 0:
            print "Query Burst"
            numqueries *= burstyquerymultiplier
        
        newquerylist = QueryGenerator.generateQueries(time, numqueries, deepstorage, DistributionFactory.createSegmentDistribution(querysegmentdistribution), minqueryperiod, maxqueryperiod, DistributionFactory.createSizeDistribution(querysizedistribution));
        Utils.printQueryList(newquerylist)
        allquerylist.extend(newquerylist)

        #Routing Queries
        for strategy in dynamicstrategies:
            strategy.routeQueries(newquerylist, segmentrunningcount, time)

    #Placing Segments
    if time % coordinatorinterval == 0:
        for strategy in dynamicstrategies:
            strategy.placeSegments(segmentlist, time, config)
        segmentlist = []

        #Print Statistics
        for strategy in dynamicstrategies:
            strategy.printStatistics(time, -1)

for strategy in dynamicstrategies:
    #Placing Segments
    strategy.placeSegments(segmentlist, time, config)
    
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

for query in allquerylist:
    query.setStartTime(0)

#Routing Queries
staticstrategy.routeQueries(allquerylist, segmentrunningcount, 0)

#Placing Segments
allsegmentlist = [item for sublist in deepstorage.values() for item in sublist]
staticstrategy.placeSegments(allsegmentlist, 0, config)

#Routing Queries
staticstrategy.routeQueries(list(), segmentrunningcount, 0)

#Print Statistics
staticstrategy.printStatistics(0, -1)
