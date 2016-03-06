#!/bin/bash

source "$1"
mkdir -p "$2"

for historicalnode in ${historicalnodecount[@]}
do
	echo "Running experiment for historicalnode: " $historicalnode
	LOG_PATH="$2"/"$historicalnode"
	echo $LOG_PATH
	mkdir -p $LOG_PATH
	RUNLOG_PATH="$LOG_PATH"/run.log
	CONF_PATH="$LOG_PATH"/tmp.conf
	{
		echo "segmentcount=$segmentcount"
		echo "querycount=$querycount"
		echo "querysegmentdistribution=${querysegmentdistribution[0]}"
		echo "querysizedistribution=${querysizedistribution[0]}"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "historicalnodecount=$historicalnode"
		echo "changesegmentdistribution=$changesegmentdistribution"
		echo "burstyquery=$burstyquery"
		echo "burstyquerymultiplier=$burstyquerymultiplier"
		echo "burstyqueryinterval=$burstyqueryinterval"
		echo "burstysegment=$burstysegment"
		echo "burstysegmentmultiplier=$burstysegmentmultiplier"
		echo "burstysegmentinterval=$burstysegmentinterval"
		echo "hottierthreshold=${hottierthreshold[0]}"
		echo "coldtierthreshold=${coldtierthreshold[0]}"
		echo "adaptivehistorycount=${adaptivehistorycount[0]}"
		echo "bestfithistorycount=${bestfithistorycount[0]}"
	} > "$CONF_PATH"

	python DynamicMain.py "$CONF_PATH" > "$RUNLOG_PATH"
	
	bash scripts/extractandplot.sh $RUNLOG_PATH $LOG_PATH
done

echo "Completed"
