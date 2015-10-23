from Node import Node

class Broker(Node):     
	
	@staticmethod
	def timeCalculation( HistNodeList, queryList):
		timeMap = []
		for x in range (0, len(HistNodeList)+1):
			timeMap.append(0)
			
		for query in queryList:
			for segment in query.segmentList:
				for node in HistNodeList:
					if node.lookup(segment) == True:
						timeMap[node.id] = timeMap[node.id]+1
						
		
		maxscore=0
		target = None
		for node in HistNodeList:
			if(timeMap[node.id]>=maxscore):
				maxscore=timeMap[node.id]
				target = node.id
		
		print "Max Score: %d" % maxscore
		print "Last node to finish ID: %d" % target
		
