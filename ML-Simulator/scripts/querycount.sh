#!/bin/bash

source "config/default.conf"
mkdir logs/querycount

for query in ${querycount[@]}
do
	echo $query
	mkdir logs/querycount/$query
	{
		echo "segmentcount=$segmentcount"
		echo "querycount=$query"
		echo "querysegmentdistribution=${querysegmentdistribution[0]}"
		echo "querysizedistribution=${querysizedistribution[0]}"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "historicalnodecount=${historicalnodecount[0]}"
		echo "placementstrategy=$placementstrategy"
		echo "replicationfactor=$replicationfactor"
		echo "percentreplicate=${percentreplicate[0]}"
	} > logs/querycount/"$query"/tmp_"$query".conf

	python Main.py logs/querycount/"$query"/tmp_"$query".conf > logs/querycount/"$query"/run_"$query".log &
done

wait

echo "Completed"
