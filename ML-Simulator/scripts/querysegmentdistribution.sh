#!/bin/bash

source "config/default.conf"
mkdir logs/querysegmentdistribution

for distribution in ${querysegmentdistribution[@]}
do
	echo "Running experiment for:" $distribution
	mkdir logs/querysegmentdistribution/$distribution
	{
		echo "segmentcount=$segmentcount"
		echo "preloadsegmenr=$preloadsegment"
		echo "querycount=${querycount[0]}"
		echo "querysegmentdistribution=$distribution"
		echo "querysizedistribution=${querysizedistribution[0]}"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "queryperinterval=${queryperinterval[0]}"
		echo "historicalnodecount=${historicalnodecount[0]}"
		echo "replicationfactor=$replicationfactor"
		echo "percentreplicate=${percentreplicate[0]}"
	} > logs/querysegmentdistribution/"$distribution"/tmp_"$distribution".conf

	python DynamicMain.py logs/querysegmentdistribution/"$distribution"/tmp_"$distribution".conf > logs/querysegmentdistribution/"$distribution"/run_"$distribution".log &
done

wait

echo "Completed"
