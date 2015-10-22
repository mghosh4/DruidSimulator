from Query import Query

class QueryGenerator(object):

	@staticmethod
	def generateQueries(segmentCount, numQueries, segmentPerQuery, distGenerator):
		count = 0
		querylist = list()
		segmentList = distGenerator.generateQuerySegments(numQueries*segmentPerQuery, segmentCount)
		#Split the segment list into each query
		for _ in xrange(numQueries):
			q = Query()
			for _ in xrange(segmentPerQuery):
				q.add(segmentList[count])
				count = count + 1
			querylist.append(q);
		return querylist
