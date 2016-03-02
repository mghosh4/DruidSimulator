from Utils import Utils
from Query import Query

class QueryGenerator(object):
	queryRunningCount = 0

	@staticmethod
	def generateQueries(time, numQueries, segmentDict, accessGenerator, minPeriod, maxPeriod, periodGenerator):
		querylist = list()
		accesslist = accessGenerator.generateDistribution(1, time, numQueries)
		#print "Segment List"
		#Utils.printlist(segmentlist)
		periodlist = periodGenerator.generateDistribution(minPeriod, maxPeriod, numQueries)
		#print "Size List"
		#Utils.printlist(sizelist)
		for i in xrange(numQueries):
			q = Query(QueryGenerator.queryRunningCount, time)
			QueryGenerator.queryRunningCount += 1
			starttime = accesslist[i]
                        if (starttime + periodlist[i] - 1 > time):
                            starttime = starttime - (periodlist[i] - (time - starttime + 1))
                        print "%d %d" % (starttime, starttime + periodlist[i])
			for j in xrange(starttime,starttime + periodlist[i]):
				for segment in segmentDict[j]:
					q.add(segment)
			querylist.append(q);
		return querylist
