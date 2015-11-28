#!/bin/bash

source "config/default.conf"
mkdir logs/querysizedistribution

for distribution in ${querysizedistribution[@]}
do
	echo $distribution
	mkdir logs/querysizedistribution/$distribution
	{
		echo "segmentcount=$segmentcount"
		echo "querycount=${querycount[0]}"
		echo "querysegmentdistribution=${querysegmentdistribution[0]}"
		echo "querysizedistribution=$distribution"
		echo "queryminsize=$queryminsize"
		echo "querymaxsize=$querymaxsize"
		echo "historicalnodecount=${historicalnodecount[0]}"
		echo "placementstrategy=$placementstrategy"
		echo "replicationfactor=$replicationfactor"
		echo "percentreplicate=${percentreplicate[0]}"
	} > logs/querysizedistribution/"$distribution"/tmp_"$distribution".conf

	python Main.py logs/querysizedistribution/"$distribution"/tmp_"$distribution".conf > logs/querysizedistribution/"$distribution"/run_"$distribution".log &
done

wait

echo "Completed"

