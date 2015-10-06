from Scheduler import Scheduler
from SchedulerRandom import RandomScheduler
from Query import Comparitor
from Placement import Placement
from PlacementMaxMatching import MatchingPlacement
from BlockLoader import BlockLoader
from BlockLoaderCostBased import BlockLoaderCostBased

NUM_NODE = 10
NUM_SLOT = 1
NUM_QUERY = 20
NUM_DATA = 1000
NUM_USED_NODE_PER_QUERY=20
SORTER = Comparitor.SIZE
SEED = 1
REPLICA_FACTOR = 3

placement = Placement() #Druid way to assign query
matchingPlacement = MatchingPlacement()
LoadStrategy = BlockLoader()

scheduler1 = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA,NUM_USED_NODE_PER_QUERY, Comparitor.SIZE, REPLICA_FACTOR, placement, LoadStrategy)
print "Hello"
metric = scheduler1.schedule()
scheduler1.printQueryList()
print "schedule: size\tplacement:random\t", metric

scheduler2 = Scheduler(NUM_NODE, NUM_SLOT, NUM_QUERY, NUM_DATA,NUM_USED_NODE_PER_QUERY,Comparitor.SIZE, REPLICA_FACTOR, matchingPlacement,LoadStrategy)
metric = scheduler2.schedule()
scheduler2.printQueryList()
print "schedule: size\tplacement:maxmatching\t", metric


