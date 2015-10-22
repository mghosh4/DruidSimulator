from Node import Node

class Broker(Node):     
	
	def timeCalculation(self, HistNodeList, queryList):
		timeMap = []
		for query in queryList:
			for segment in query.segmentList:
				for node in HistNodeList:
					if node.lookup(segment.time) == true:
						timeMap[node.id]++
						
		
		maxscore=0
		for node in HistNodeList:
			if(timeMap[node.id]>=maxscore):
				maxscore=timeMap[node.id]
		
		return maxscore
