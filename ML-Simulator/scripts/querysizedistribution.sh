#!/bin/bash

source "config/default.conf"
mkdir logs/querycount

for query in ${querycount[@]}
do
	echo "Running experiment for:" $query
	mkdir logs/querycount/$query
	{
		echo "segmentcount=$segmentcount"
		echo "preloadsegmenr=$preloadsegment"
		echo "querycount=$query"
		echo "querysegmentdistribution=${querysegmentdistribution[0]}"
		echo "querysizedistribution=${querysizedistribution[0]}"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "queryperinterval=${queryperinterval[0]}"
		echo "historicalnodecount=${historicalnodecount[0]}"
		echo "replicationfactor=$replicationfactor"
		echo "percentreplicate=${percentreplicate[0]}"
	} > logs/querycount/"$query"/tmp_"$query".conf

	python DynamicMain.py logs/querycount/"$query"/tmp_"$query".conf > logs/querycount/"$query"/run_"$query".log &
done

wait

echo "Completed"
