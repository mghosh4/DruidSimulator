#!/bin/bash

source "config/default.conf"
mkdir logs/queryperinterval

for interval in ${queryperinterval[@]}
do
	echo "Running experiment for:" $interval
	mkdir logs/queryperinterval/$interval
	{
		echo "segmentcount=$segmentcount"
		echo "preloadsegmenr=$preloadsegment"
		echo "querycount=${querycount[0]}"
		echo "querysegmentdistribution=${querysegmentdistribution[0]}"
		echo "querysizedistribution=${querysizedistribution[0]}"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "queryperinterval=$interval"
		echo "historicalnodecount=${historicalnodecount[0]}"
		echo "replicationfactor=$replicationfactor"
		echo "percentreplicate=${percentreplicate[0]}"
	} > logs/queryperinterval/"$interval"/tmp_"$interval".conf

	python DynamicMain.py logs/queryperinterval/"$interval"/tmp_"$interval".conf > logs/queryperinterval/"$interval"/run_"$interval".log &
done

wait

echo "Completed"
