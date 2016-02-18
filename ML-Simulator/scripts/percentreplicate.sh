#!/bin/bash

source "config/default.conf"
mkdir logs/percentreplicate

for percent in ${percentreplicate[@]}
do
	echo "Running experiment for:" $percent
	mkdir logs/percentreplicate/$percent
	{
		echo "segmentcount=$segmentcount"
		echo "preloadsegmenr=$preloadsegment"
		echo "querycount=${querycount[0]}"
		echo "querysegmentdistribution=${querysegmentdistribution[0]}"
		echo "querysizedistribution=${querysizedistribution[0]}"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "queryperinterval=${queryperinterval[0]}"
		echo "historicalnodecount=${historicalnodecount[0]}"
		echo "replicationfactor=$replicationfactor"
		echo "percentreplicate=$percent"
	} > logs/percentreplicate/"$percent"/tmp_"$percent".conf

	python DynamicMain.py logs/percentreplicate/"$percent"/tmp_"$percent".conf > logs/percentreplicate/"$percent"/run_"$percent".log &
done

wait

echo "Completed"
