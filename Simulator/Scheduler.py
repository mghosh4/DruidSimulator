from Query import Query, QueryGenerator, Comparitor, DISTRIBUTION
import DataBlock
from Node import NodeGenerator,Node
import Queue
import math
import random


class Scheduler(object):
    def __init__(self, NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA, NUM_USED_NODE, SORTER, REPLICA_FACTOR, placement, LoadStrategy):
        self.NUM_NODE = NUM_NODE
        self.NUM_SLOT = NUM_SLOT
        self.NUM_QUERY = NUM_QUERY
        self.NUM_DATA = NUM_DATA
        self.SORTER = SORTER
        self.NUM_USED_NODE = NUM_USED_NODE
        self.placement = placement
        self.REPLICA_FACTOR = REPLICA_FACTOR
        self.loader=LoadStrategy
        self.loaded_blocks = []
        self.block_map = {}
        self.setup()
        
    
    def setup(self):
        #Generate Data blocks
        self.data_gen = DataBlock.DataBlockGenerator(self.NUM_DATA,1)
        self.data_gen.generate()

        #Generate Nodes
        self.nodeGen = NodeGenerator(self.NUM_NODE, self.NUM_SLOT)
        self.nodeGen.generate()
        self.init_queue()

        #Generate Query
        self.query_gen = QueryGenerator(self.data_gen.dataList, self.NUM_QUERY,1,1)
        self.query_gen.generate(self.SORTER, DISTRIBUTION.MIX, 40, 0)
        #self.query_gen.printAll()

        #Load data into node
        self.load_data_to_node(self.data_gen.dataList, self.nodeGen.nodeList, self.loader)

    def init_queue(self):
        self.node_queue = Queue.PriorityQueue()
        #node_queue.put(nodeGen.nodeList)
        map(self.node_queue.put, self.nodeGen.nodeList)

    def load_data_to_node(self, dataList, nodeList, loader):
        self.block_map = loader.load(dataList, nodeList,  self.REPLICA_FACTOR)


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

                node_list = []
                num_used_node = min(self.NUM_USED_NODE , self.NUM_NODE)
                
                for i in range(0,num_used_node):
                    node = self.node_queue.get()
                    node_list.append(node)
                    if self.node_queue.qsize() == 0:
                        break

                assignment = self.placement.assign(node_list,query.dataList, self.block_map)
               
                for k,v in assignment.items():
                    (executionTime,completionTime) = k.execute(v)
		    '''   
                    print k.id, " :",
                    for block in v:
                        print block.id, ",",
                    print "completion time:", completionTime
                    '''
                    query.completionTime = query.completionTime if query.completionTime > completionTime else completionTime
                    self.node_queue.put(k)

                print "Dealing with Query: ", query.show()
                totalCompletionTime += query.completionTime 

        return totalCompletionTime/num_query;

    def printQueryList(self):       
        for query in self.query_gen.taskList:
             print query.show()     
        

