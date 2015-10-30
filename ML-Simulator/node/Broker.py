from Node import Node
import logging

class Broker(Node):     
	
	@staticmethod
	def timeCalculation( HistNodeList, queryList, placementstrategy):
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
		
		
		print "Placement Strategy: " + placementstrategy
		print "Max Score: %d" % maxscore
		print "Last node to finish ID: %d" % target
		logging.basicConfig(filename='FinalScore.log',level=logging.DEBUG)
		logging.debug('This message should go to the log file')
		logging.info('So should this')
		logging.warning('And this, too')
		print "Placement Strategy: " + placementstrategy
		print "Max Score: %d" % maxscore
		print "Last node to finish ID: %d" % target

