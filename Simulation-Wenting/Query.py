import DataBlock
import random

class Comparitor:
    SIZE, RECENCY, PRIORITY, RANDOM = range(4)

class DISTRIBUTION:
    RANDOM, EXP, ZIPF, UNIFORM, REPEAT, MIX= range(6)


class Query(object):
    def __init__(self, id, dataList, startTime, endTime, size):
        self.id = id
        self.dataList = dataList
        self.startTime = startTime
        self.endTime = endTime
        self.size = size
        self.completionTime = 0
    
    def show(self):
        return str(self.id) + ': '+ str(self.startTime)+'->'+str(self.endTime)+'\tsize:'+ str(self.size)+' priority:'+ str(self.size*self.startTime) + '\tcompletionTime:'+ str(self.completionTime)
        #for data in self.dataList:
        #    data.show()
        #print ""

class QueryGenerator(object):
    def __init__(self, dataBlockPool, totalNum, sizeDistribution, recencyDistribution):
         self.dataBlockPool = dataBlockPool
         self.totalNum = totalNum
         self.sizeDistribution = sizeDistribution
         self.recencyDistribution = recencyDistribution
         self.taskList = []

    def sort(self, comparitor):
        if comparitor == Comparitor.SIZE:
            self.taskList.sort(key=lambda query: query.size)
        elif comparitor == Comparitor.RECENCY:
            self.taskList.sort( key=lambda query: query.startTime)
        elif comparitor == Comparitor.PRIORITY:
            self.taskList.sort( cmp= priority_compare)
        elif comparitor == Comparitor.RANDOM:
            random.shuffle(self.taskList)
        else:
            return

    def generate(self, sortComparitor, query_distribution, median, repeat_percent=None):
        random.seed(1)
        if query_distribution == DISTRIBUTION.RANDOM:
            for i in range(self.totalNum):
                startTime = random.randrange(len(self.dataBlockPool))
                if startTime <= 0:
                    i = i - 1
                    continue
                endTime = startTime - random.randrange(startTime)
                self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime,startTime-endTime))
	elif query_distribution == DISTRIBUTION.EXP:
            for i in range(self.totalNum):
            	task_duration = int(random.expovariate(1.0 / median))
		startTime = random.randint(task_duration, len(self.dataBlockPool))
                endTime = startTime - task_duration
            	self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime, task_duration))
        elif query_distribution == DISTRIBUTION.UNIFORM:
            for i in range(self.totalNum):
                task_duration = int(random.uniform(0,len(self.dataBlockPool)))
		startTime = random.randint(task_duration, len(self.dataBlockPool))
                endTime = startTime - task_duration
                if endTime >=0: 
            	    self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime, task_duration))
        elif query_distribution == DISTRIBUTION.REPEAT:
            for i in range(self.totalNum):
                task_duration = median
		startTime = len(self.dataBlockPool)-1
                endTime = startTime - task_duration
                if endTime >=0: 
            	    self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime, task_duration))
        elif query_distribution == DISTRIBUTION.MIX:
             if repeat_percent is None:
                 repeat_percent = 0.5
             exp_num = int(self.totalNum *(1- repeat_percent))
             repeat_num = int( self.totalNum * repeat_percent)
             print "repeat:",repeat_num, "other:", exp_num
             for i in range(repeat_num):
                 task_duration = median
		 startTime = len(self.dataBlockPool)-1
                 endTime = startTime - task_duration
                 if endTime >=0: 
            	     self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime, task_duration))
             for i in range(exp_num):
                 task_duration = int(random.expovariate(1.0 / median))
		 startTime = random.randint(task_duration, len(self.dataBlockPool))
                 endTime = startTime - task_duration
            	 self.taskList.append(Query(i,self.dataBlockPool[endTime:startTime],startTime,endTime, task_duration))

   
        self.sort(sortComparitor)
        

    def printAll(self):
         for query in self.taskList:
             print query.show()



def priority_compare(q1,q2):
    return q1.startTime * q1.size - q2.startTime * q2.size
'''
Datagen = DataBlock.DataBlockGenerator(1000,1)
Datagen.generate()
gen = QueryGenerator(Datagen.dataList, 1000,1,1)
comparitor = Comparitor.SIZE
distribution = DISTRIBUTION.EXP
median = 100
gen.generate(comparitor,distribution, median, 0.6)
gen.printAll()
'''
