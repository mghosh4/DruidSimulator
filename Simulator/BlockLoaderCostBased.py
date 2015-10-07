from Query import Query, QueryGenerator, Comparitor
from Node import NodeGenerator,Node
import math
import Queue
import DataBlock
from BlockLoader import BlockLoader


class BlockLoaderCostBased(BlockLoader):
   
    #Cost Loaded block assignment, similar to Druid strategy
    def load(self, dataList, nodeList, REPLICA_FACTOR):

        for proposalBlock in dataList:
            self.block_map[proposalBlock]=[]
            for i in range(REPLICA_FACTOR): 
                minCost = 1000.0
                for node in nodeList:
                    cost = 0
                    #print node.id
                    for loadedBlock in node.memory:
                         #if loadedBlock != proposalBlock:
                         cost = cost+ self.blockCost(proposalBlock, loadedBlock)
                    if cost < minCost:
                       #print cost, ", ", minCost
                       minCost = cost
                       proposalNode = node

                proposalNode.memory.append(proposalBlock)
                self.block_map[proposalBlock].append(proposalNode)
        return self.block_map


    def blockCost(self, proposalBlock, block):
        base = min(proposalBlock.computationTime, block.computationTime)
        # if from the same data source, it is 2
        dataSourcePenalty = 1
        # recency penalty
        recencyPenalty = 1
        #gap
        gapPenalty = 1
        if proposalBlock != block:
            gapPenalty = 1/(math.fabs(proposalBlock.startTime-block.startTime))
        else:
            dataSourcePenalty = 2
       
        cost = base * dataSourcePenalty * recencyPenalty * gapPenalty
        #print cost
        return cost

'''
#test
nodeGen = NodeGenerator(10,1)
nodeGen.generate()
nodeGen.printAll()

Datagen = DataBlock.DataBlockGenerator(20,1)
Datagen.generate()
Datagen.printAll()

loader = BlockLoaderCostBased()
block_map=loader.load(Datagen.dataList,nodeGen.nodeList, 3)

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
