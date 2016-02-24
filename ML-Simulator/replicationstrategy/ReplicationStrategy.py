import math
import sys
from collections import Counter
from Queue import PriorityQueue

class Fixed(object):
    REPLICATION_FACTOR = 2
    def replicateSegments(self, segmentList, deepStorage, historicalNodeList, queryList, segmentCount):
        insertlist = list()
        for segment in segmentList:
            if segmentCount[segment] <= 1:
                for _ in xrange(0, Fixed.REPLICATION_FACTOR - segmentCount[segment]):
                    insertlist.append(segment)

        return (insertlist, list())

class Tiered(object):
    REPLICATION_FACTOR = 2
    def replicateSegments(self, segmentList, deepStorage, historicalNodeList, queryList, segmentCount):
        insertlist = list()
        removelist = list()

        for segment in segmentList:
            if segmentCount[segment] <= 1:
                for _ in xrange(0, Tiered.REPLICATION_FACTOR - segmentCount[segment]):
                    insertlist.append(segment)

        for segment in segmentCount.iterkeys():
            if segment not in segmentList and segmentCount[segment] > 1:
                for _ in xrange(0, segmentCount[segment] - 1):
                    removelist.append(segment)

        return (insertlist, removelist)

class Adaptive(object):
    def replicateSegments(self, segmentList, deepStorage, historicalNodeList, queryList, segmentCount):
        insertlist = list()
        removelist = list()

        segmenttimecount = Counter()
	for query in queryList:
	    segmenttimecount += query.getSegmentTimeCount()

        totalqueriedsegments = sum(segmenttimecount.values())
        for time in segmenttimecount.iterkeys():
            segment = deepStorage[time - 1]
            currentsegmentcount = segmentCount[segment]
            expectedsegmentcount = math.ceil(float(segmenttimecount[time] * len(historicalNodeList)) / float(totalqueriedsegments))

            if (expectedsegmentcount == 0):
                for _ in xrange(0, currentsegmentcount):
                    removelist.append(segment)
            elif (currentsegmentcount > expectedsegmentcount):
                for _ in xrange(0, currentsegmentcount - int(expectedsegmentcount)):
                    removelist.append(segment)
            elif (currentsegmentcount < expectedsegmentcount):
                for _ in xrange(0, int(expectedsegmentcount) - currentsegmentcount):
                    insertlist.append(segment)

        for segment in segmentCount.iterkeys():
            if segment.getTime() not in segmenttimecount and segmentCount[segment] > 0:
                for _ in xrange(0, segmentCount[segment]):
                    removelist.append(segment)

        return (insertlist, removelist)

class BestFit(object):
    def replicateSegments(self, segmentList, deepStorage, historicalNodeList, queryList, segmentCount):
        insertlist = list()
        removelist = list()

        segmenttimecount = Counter()
	for query in queryList:
	    segmenttimecount += query.getSegmentTimeCount()

        totalqueriedsegments = sum(segmenttimecount.values())
	slotsperhn = math.ceil(float(totalqueriedsegments) / len(historicalNodeList))
        nodecapacities = list()
        for i in xrange(0,len(historicalNodeList)):
            nodecapacities.append(slotsperhn)

        maxheap = PriorityQueue()
        for (key, value) in segmenttimecount.items():
            maxheap.put((value, key))

        while not maxheap.empty():
            (val, key) = maxheap.get()
            valleft = self.bestfit(val, nodecapacities)
            if (valleft > 0):
                insertlist.append(deepStorage[key - 1])
                maxheap.put((valleft, key))

        for segment in segmentCount.iterkeys():
            if segment.getTime() not in segmenttimecount and segmentCount[segment] > 0:
                for _ in xrange(0, segmentCount[segment]):
                    removelist.append(segment)

        return (insertlist, removelist)

    def bestfit(self, val, nodeCapacities):
        mincapleftafterfill = sys.maxsize
        minvalleftafterfill = sys.maxsize
        minfitindex = 0
        minspillindex = 0
        counter = 0
        fits = False
        for capacity in nodeCapacities:
            if (val <= capacity):
                fits =  True
                leftafterfill = capacity - val
                if (leftafterfill < mincapleftafterfill):
                    mincapleftafterfill = leftafterfill
                    minfitindex = counter
            else:
                leftafterfill = val - capacity
                if (leftafterfill < minvalleftafterfill):
                    minvalleftafterfill = leftafterfill
                    minspillindex = counter
            counter += 1

        if fits == True:
            nodeCapacities[minfitindex] = mincapleftafterfill
            return 0
        else:
            nodeCapacities[minspillindex] = 0
            return minvalleftafterfill

