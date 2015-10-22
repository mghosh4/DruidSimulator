from enum import Enum

class Utils(object):

	#node type:
	BROKER = "Broker"
	COORDINATOR = "Coordnator"
	HISTORICAL = "Historical"
	REALTIME = "Realtime"

class Distribution(Enum):
	UNIFORM = 1
	ZIPF = 2
	LATEST = 3
