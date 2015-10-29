from Utils import Utils
from Query import Query

class QueryGenerator(object):

	@staticmethod
	def generateQueries(numQueries, segmentCount, segmentGenerator, minSize, maxSize, sizeGenerator):
		querylist = list()
		segmentlist = segmentGenerator.generateDistribution(1, segmentCount, numQueries)
		print "Segment List"
		Utils.printlist(segmentlist)
		sizelist = sizeGenerator.generateDistribution(minSize, maxSize, numQueries)
		print "Size List"
		Utils.printlist(sizelist)
		for i in xrange(numQueries):
			q = Query()
			startsegment = 0
			chosensegment = segmentlist[i]
			if chosensegment + sizelist[i] - 1 > segmentCount:
				startsegment = chosensegment - (sizelist[i] - (segmentCount - chosensegment + 1))
			else:
				startsegment = chosensegment
			for j in xrange(startsegment,startsegment + sizelist[i]):
				q.add(j)
			querylist.append(q);
		return querylist
