import os,sys
import time
sys.path.append(os.path.abspath('query'))
sys.path.append(os.path.abspath('distributiongenerators'))

from Query import Query
from QueryGenerator import QueryGenerator
from UniformDistributionGenerator import UniformDistributionGenerator
from ZipfDistributionGenerator import ZipfDistributionGenerator
from LatestDistributionGenerator import LatestDistributionGenerator

uniformList = QueryGenerator.generateQueries(10, 5, 3, UniformDistributionGenerator());
print "Uniform"
for query in uniformList:
	query.info()


zipfList = QueryGenerator.generateQueries(10, 5, 3, ZipfDistributionGenerator());
print "Zipfian"
for query in zipfList:
	query.info()


latestList = QueryGenerator.generateQueries(10, 5, 3, LatestDistributionGenerator());
print "Latest"
for query in latestList:
	query.info()
