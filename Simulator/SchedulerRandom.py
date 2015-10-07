from Scheduler import Scheduler
import itertools
import sys
import copy
import random

class RandomScheduler(Scheduler):
     def __init__(self, NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, NUM_USED_NODE, SORTER, REPLICA_FACTOR, placement):
        super(RandomScheduler, self).__init__(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, NUM_USED_NODE, SORTER,  REPLICA_FACTOR, placement)
        self.node_queue = set(self.nodeGen.nodeList)
        
        
     def schedule(self, taskList=None):
        #maintain a priority queue based on available nodes(who has the smallest current time)
        #for each query
        #    do round robin
        totalCompletionTime = 0.0
        totalExecutionTime = 0.0
        if taskList is None:
           taskList = self.query_gen.taskList
        num_query = len(taskList)
        #Layer1 scheduler: schedule the order of queries
        #TODO reschedule the task list if needed

        #Layer2 scheduler: the placement/assignment of queries to nodes
        for query in taskList:
            if query.size != 0:
                '''
                # Replicate datablock when first time is queried.
                for block in query.dataList:
                    if block not in self.loaded_blocks:
                        self.loaded_blocks.append(block)
                        #random choose some node to load data block
                        replica_node_list= random.sample(self.node_queue,self.REPLICA_FACTOR)
              
                        for node in replica_node_list:
                            node.memory.append(block)
                            #self.node_queue.add(node) no need
                            #node.show()
                '''         
                node_list = []
                num_used_node = min(self.NUM_USED_NODE, self.NUM_NODE)
                #randomly choose some nodes
                node_list= random.sample(self.node_queue,num_used_node)
    		
                assignment = self.placement.assign(node_list,query.dataList)
                for k,v in assignment.items():
                    
                    (executionTime,completionTime) = k.execute(v)
                    '''
                    print k.id, " :",
                    for block in v:
                        print block.id, ",",
                    print "completion time:", completionTime
                    '''
                    query.completionTime = query.completionTime if query.completionTime > completionTime else completionTime
                    self.node_queue.add(k)
                print "random queue size",len(self.node_queue),"Dealing with Query: ", query.show()
                totalCompletionTime += query.completionTime 
        return totalCompletionTime/num_query;
