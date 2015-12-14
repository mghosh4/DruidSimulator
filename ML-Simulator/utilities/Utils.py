class Utils(object):

	GAP_THRESHOLD = 10
	RECENCY_THRESHOLD = 5 

	@staticmethod
	def printlist(listforprint):
		print ', '.join(str(x) for x in listforprint)

	@staticmethod
	def printQueryList(queryList):
		for query in queryList:
			query.show()

	@staticmethod
	def printSegmentPlacement(historicalNodeList):
		for node in historicalNodeList:
			node.printSegmentList()

	@staticmethod
	def printQueryAssignment(historicalNodeList):
		for node in historicalNodeList:
			node.printQueryList()
