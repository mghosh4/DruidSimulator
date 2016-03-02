import math
import sys
from collections import Counter
from Queue import PriorityQueue

class Fixed(object):
    REPLICATION_FACTOR = 2
    def replicateSegments(self, segmentList, historicalNodeList, queryList, segmentCount, pastHistory, time):
        insertlist = list()
        for segment in segmentList:
            if segmentCount[segment] <= 1:
                for _ in xrange(0, Fixed.REPLICATION_FACTOR - segmentCount[segment]):
                    insertlist.append(segment)

        return (insertlist, list())

class Tiered(object):
    HOT_TIER_REPLICATION = 2
    COLD_TIER_REPLICATION = 1
    HOT_TIER_THRESHOLD = 300
    COLD_TIER_THRESHOLD = 800
    def replicateSegments(self, segmentList, historicalNodeList, queryList, segmentCount, pastHistory, time):
        insertlist = list()
        removelist = list()

        for segment in segmentList:
            if segmentCount[segment] < Tiered.HOT_TIER_REPLICATION:
                for _ in xrange(0, Tiered.HOT_TIER_REPLICATION - segmentCount[segment]):
                    insertlist.append(segment)

        for segment in segmentCount.iterkeys():
            if segment not in segmentList:
                removed = segment.getTime() <= (time - Tiered.COLD_TIER_THRESHOLD)
                if removed == True:
                    for _ in xrange(0, segmentCount[segment]):
                        removelist.append(segment)
                else:
                    incoldtier = segment.getTime() <= (time - Tiered.HOT_TIER_THRESHOLD)
                    if incoldtier == True and segmentCount[segment] > Tiered.COLD_TIER_REPLICATION:
                        for _ in xrange(0, segmentCount[segment] - Tiered.COLD_TIER_REPLICATION):
                            removelist.append(segment)

        querysegmentcount = Counter()
	for query in queryList:
	    querysegmentcount += query.getSegmentList()

        for segment in querysegmentcount.iterkeys():
            removed = segment.getTime() <= (time - Tiered.COLD_TIER_THRESHOLD)
            if removed == True:
                if segment not in insertlist and segmentCount[segment] == 0:
                    for _ in xrange(0, Tiered.COLD_TIER_REPLICATION):
                        insertlist.append(segment)
                elif segment in removelist and segmentCount[segment] > 0:
                    for _ in xrange(0, segmentCount[segment]):
                        removelist.remove(segment)

        return (insertlist, removelist)

class Adaptive(object):
    HISTORY_COUNT = 5
    def replicateSegments(self, segmentList, historicalNodeList, queryList, segmentCount, pastHistory, time):
        insertlist = list()
        removelist = list()

        querysegmentcount = Counter()
	for query in queryList:
	    querysegmentcount += query.getSegmentList()

	segmentunion = querysegmentcount.keys()
	for historycount in pastHistory:
	    segmentunion = list(set().union(segmentunion, historycount.keys()))
	
	segmentpopularitymap = dict()
	for segment in segmentunion:
	    count = 0
	    value = float(querysegmentcount[segment]) / pow(2, count)
	    count += 1

	    for historycount in reversed(pastHistory):
		value += float(historycount[segment]) / pow(2, count)
		count += 1

            segmentpopularitymap[segment] = value

	pastHistory.append(querysegmentcount)
        if (len(pastHistory) > Adaptive.HISTORY_COUNT):
	    pastHistory.pop(0)

        print segmentpopularitymap
        totalqueriedsegments = sum(segmentpopularitymap.values())
        for segment in segmentpopularitymap.iterkeys():
            currentsegmentcount = segmentCount[segment]
            expectedsegmentcount = math.ceil(segmentpopularitymap[segment] * len(historicalNodeList) / float(totalqueriedsegments))

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
            if segment not in segmentpopularitymap.keys() and segmentCount[segment] > 0:
                for _ in xrange(0, segmentCount[segment]):
                    removelist.append(segment)

        return (insertlist, removelist)

class BestFit(object):
    HISTORY_COUNT = 5
    def replicateSegments(self, segmentList, historicalNodeList, queryList, segmentCount, pastHistory, time):
        insertlist = list()
        removelist = list()

        querysegmentcount = Counter()
	for query in queryList:
	    querysegmentcount += query.getSegmentList()

	segmentunion = querysegmentcount.keys()
	for historycount in pastHistory:
	    segmentunion = list(set().union(segmentunion, historycount.keys()))
	
	segmentpopularitymap = dict()
	for segment in segmentunion:
	    count = 0
	    value = float(querysegmentcount[segment]) / pow(2, count)
	    count += 1

	    for historycount in reversed(pastHistory):
		value += float(historycount[segment]) / pow(2, count)
		count += 1

            segmentpopularitymap[segment] = math.ceil(value)

	pastHistory.append(querysegmentcount)
        if (len(pastHistory) > BestFit.HISTORY_COUNT):
	    pastHistory.pop(0)

        totalqueriedsegments = sum(segmentpopularitymap.values())
	slotsperhn = math.ceil(float(totalqueriedsegments) / len(historicalNodeList))
        nodecapacities = list()
        for i in xrange(0,len(historicalNodeList)):
            nodecapacities.append(slotsperhn)

        maxheap = PriorityQueue()
        for (key, value) in segmentpopularitymap.items():
            maxheap.put((value, key))

	expectedCount = Counter()
        while not maxheap.empty():
            (val, key) = maxheap.get()
            valleft = self.bestfit(val, nodecapacities)
            expectedCount[key] += 1
            if (valleft > 0):
                maxheap.put((valleft, key))

	for segment in expectedCount.iterkeys():
	    currentsegmentcount = segmentCount[segment]
	    expectedsegmentcount = expectedCount[segment]
            if (currentsegmentcount > expectedsegmentcount):
                for _ in xrange(0, currentsegmentcount - expectedsegmentcount):
                    removelist.append(segment)
            elif (currentsegmentcount < expectedsegmentcount):
                for _ in xrange(0, expectedsegmentcount - currentsegmentcount):
                    insertlist.append(segment)

        for segment in segmentCount.iterkeys():
            if segment not in segmentpopularitymap.keys() and segmentCount[segment] > 0:
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

