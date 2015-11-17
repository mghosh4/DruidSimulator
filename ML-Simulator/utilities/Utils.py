class Utils(object):

	GAP_THRESHOLD = 10
	RECENCY_THRESHOLD = 5 

	@staticmethod
	def printlist(listforprint):
		print ', '.join(str(x) for x in listforprint)
