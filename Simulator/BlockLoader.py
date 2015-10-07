from Query import Query, QueryGenerator, Comparitor
from Node import NodeGenerator,Node
import math
import Queue
import DataBlock
import random

class BlockLoader(object):
    def __init__(self):
        self.block_map = {}

    #Random Loaded
    def load(self, dataList, nodeList, REPLICA_FACTOR):
        for block in dataList:
            #random choose some node to load data block
            replica_node_list= random.sample(nodeList,REPLICA_FACTOR)
            self.block_map[block] = []
            for node in replica_node_list:
                node.memory.append(block)
                self.block_map[block].append(node)
        return self.block_map
'''
#test
nodeGen = NodeGenerator(10,1)
nodeGen.generate()
nodeGen.printAll()

Datagen = DataBlock.DataBlockGenerator(20,1)
Datagen.generate()
Datagen.printAll()

loader = BlockLoader()
block_map = loader.load(Datagen.dataList,nodeGen.nodeList, 3)

for node in nodeGen.nodeList:
   print node.id, " :[",
   for block in node.memory:
       print block.id, " ",
   print "]"

for k,v in block_map.items():
   print k.id, " :[",
   for node in v:
       print node.id, " ",
   print "]"
'''
