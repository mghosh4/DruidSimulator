import os,sys
import time
sys.path.append(os.path.abspath('query'))
sys.path.append(os.path.abspath('distribution'))
sys.path.append(os.path.abspath('config'))

from ParseConfig import ParseConfig
from Query import Query
from QueryGenerator import QueryGenerator
from DistributionFactory import *

def testDistributionCode():
	uniformList = QueryGenerator.generateQueries(10, 5, 3, Uniform());
	print "Uniform"
	for query in uniformList:
		query.info()
	
	zipfList = QueryGenerator.generateQueries(10, 5, 3, Zipfian());
	print "Zipfian"
	for query in zipfList:
		query.info()
	
	latestList = QueryGenerator.generateQueries(10, 5, 3, Latest());
	print "Latest"
	for query in latestList:
		query.info()

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

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

config.printConfig()

segmentCount = config.getSegmentCount()
queryCount = config.getQueryCount()
segmentPerQuery = config.getSegmentPerQuery()
distribution = config.getDistribution()

querylist = QueryGenerator.generateQueries(segmentCount, queryCount, segmentPerQuery, DistributionFactory.createDistribution(distribution));
for query in querylist:
	query.info()
