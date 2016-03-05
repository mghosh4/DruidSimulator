#!/bin/bash

source "$1"
mkdir -p "$2"

for threshold in ${hottierthreshold[@]}
do
		echo "Running experiment for hottierthreshold: " $threshold
		LOG_PATH="$2"/"$threshold
		echo $LOG_PATH
		mkdir -p $LOG_PATH
		RUNLOG_PATH="$LOG_PATH"/run.log
		CONF_PATH="$LOG_PATH"/tmp.conf
		{
			echo "segmentcount=$segmentcount"
			echo "querycount=$querycount"
			echo "querysegmentdistribution=${segmentdistribution[0]}"
			echo "querysizedistribution=${sizedistribution[0]}"
			echo "queryminsize=$queryminsize"
			echo "querymaxsize=$querymaxsize"
			echo "historicalnodecount=${historicalnodecount[0]}"
			echo "changesegmentdistribution=$changesegmentdistribution"
			echo "burstyquery=$burstyquery"
			echo "burstyquerymultiplier=$burstyquerymultiplier"
			echo "burstyqueryinterval=$burstyqueryinterval"
			echo "burstysegment=$burstysegment"
			echo "burstysegmentmultiplier=$burstysegmentmultiplier"
			echo "burstysegmentinterval=$burstysegmentinterval"
			echo "hottierthreshold=$threshold"
			echo "coldtierthreshold=${coldtierthreshold[0]}"
			echo "adaptivehistorycount=${adaptivehistorycount[0]}"
			echo "bestfithistorycount=${bestfithistorycount[0]}"
		} > "$CONF_PATH"

		python DynamicMain.py "$CONF_PATH" > "$RUNLOG_PATH"

		bash scripts/grepdata.sh Replicas $RUNLOG_PATH > "$LOG_PATH"/replicas.log
		bash scripts/grepdata.sh Throughput $RUNLOG_PATH | tail -1 > "$LOG_PATH"/throughput.log
		bash scripts/grepdata.sh Factor $RUNLOG_PATH > "$LOG_PATH"/factor.log
		bash scripts/grepdata.sh Loads $RUNLOG_PATH | tail -1 > "$LOG_PATH"/loads.log
		bash scripts/grepdata.sh Completion $RUNLOG_PATH | tail -1 > "$LOG_PATH"/completion.log
done

wait

echo "Completed"
