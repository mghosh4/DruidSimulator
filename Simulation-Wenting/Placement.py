from Query import Query, QueryGenerator, Comparitor
from Node import NodeGenerator,Node
import math
import Queue
import DataBlock
import random

class Placement(object):
     
     # druid assignment
     def assign(self, node_list, block_list, block_map):
         #log

         #druid assignment
         placement = {}
         for block in block_list:
             replica_list = block_map[block]
             node = replica_list[random.randrange(0,len(replica_list))]
             if node not in placement:
                 placement[node]=[block]
             else:
                 placement[node].append(block)

         print "Druid--------"
	 total_inMemory = 0
         '''
	 for k,v in placement.items():
             inMemory_count = 0
	     print "id:",k.id,":[",
	     for block in v:
                 if block in k.memory:
	             print block.id, "* ",
                     inMemory_count+=1
                 else:
                     print block.id, " ",
	     print "] In Memory Count=", inMemory_count, "Not in Memory Count=", len(v)-inMemory_count
             total_inMemory += inMemory_count
         print "random for this query: total in Memory:" ,total_inMemory
         '''
         return placement
'''
#test
nodeGen = NodeGenerator(4,1)
nodeGen.generate()
nodeGen.printAll()

Datagen = DataBlock.DataBlockGenerator(13,1)
Datagen.generate()
Datagen.printAll()

placement = Placement()
result = placement.assign(nodeGen.nodeList,Datagen.dataList)

for k,v in result.items():
   print k.id, " :",
   for block in v:
       print block.id, ",",
   print
'''
