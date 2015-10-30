from ReplicationStrategy import ReplicationStrategy
from HistoricalNode import HistoricalNode
from copy import deepcopy

class DruidReplicationStrategy(ReplicationStrategy):
	
	
	@staticmethod
	def replicateSegments(self, historicalNodeList, primary, strategy, replicationFactor):
		if strategy == "balance":
			return balance(historicalNodeList, primary, replicationFactor)
	
	@staticmethod	
	def balance(self, historicalNodeList, primary, replicationFactor):
		lst = deepcopy(historcialNodeList)
		
		if lst[0].id == primary.id:
			start = 1
		else:
			start = 0
			
		if replicationFactor > len(historcialNodeList):
			replicationFactor = len(historcialNodeList)
			
			
		for i in range(start, replicationFactor-1):
			min_len = lst[i].queue_size()
			min_idx = i
			for j in range(i+1, len(lst)-1):
				if lst[j].queue_size() < min_len and hn.id != primary.id:
					min_idx = i
					min_len = lst[j].queue_size()
			targetNodeId.append(lst[min_len].id)
			tmp = deepcopy(lst[i])
			lst[i] = deepcopy(lst[min_len])
			lst[min_len] = tmp
			
		return targetNodeId
			
					
				
			