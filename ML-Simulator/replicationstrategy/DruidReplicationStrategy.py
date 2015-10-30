from ReplicationStrategy import ReplicationStrategy
from HistoricalNode import HistoricalNode
from copy import deepcopy

class DruidReplicationStrategy(ReplicationStrategy):
	
	
	@staticmethod
	def replicateSegments( historicalNodeList, primary, strategy, replicationFactor):
		if strategy == "balance":
			return DruidReplicationStrategy.balance(historicalNodeList, primary, replicationFactor)
	
	@staticmethod	
	def balance( historicalNodeList, primary, replicationFactor):
		if historicalNodeList[0].id == primary.id:
			start = 1
		else:
			start = 0
			
		if replicationFactor > len(historicalNodeList):
			replicationFactor = len(historicalNodeList)
			
		targetNodeId = []
			
		for i in range(start, start+replicationFactor-1):
			min_len = historicalNodeList[i].queue_size() + historicalNodeList[i].replica_size()
			min_idx = i
			for j in range(i, len(historicalNodeList)):
				size = historicalNodeList[j].queue_size()
				if historicalNodeList[j].queue_size() + historicalNodeList[j].replica_size() < min_len and historicalNodeList[j].id != primary.id:
					min_idx = j
					min_len = historicalNodeList[j].queue_size() + historicalNodeList[j].replica_size()
					
			targetNodeId.append(historicalNodeList[min_idx].id)
			historicalNodeList[i], historicalNodeList[min_idx] = historicalNodeList[min_idx], historicalNodeList[i]
			
		return targetNodeId
			
					
				
			