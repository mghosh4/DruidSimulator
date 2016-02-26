#!/bin/bash

source "$1"
mkdir -p "$2"/querysegmentandsizedistribution

for segmentdistribution in ${querysegmentdistribution[@]}
do
	for sizedistribution in ${querysizedistribution[@]}
	do
		echo "Running experiment for segmentdistribution: " $segmentdistribution " sizedistribution: " $sizedistribution
		mkdir -p "$2"/querysegmentandsizedistribution/"$segmentdistribution"/"$sizedistribution"
		{
			echo "segmentcount=$segmentcount"
			echo "preloadsegment=$preloadsegment"
			echo "querycount=${querycount[0]}"
			echo "querysegmentdistribution=$segmentdistribution"
			echo "querysizedistribution=$sizedistribution"
			echo "queryminsize=$queryminsize"
			echo "querymaxsize=$querymaxsize"
			echo "queryperinterval=${queryperinterval[0]}"
			echo "historicalnodecount=${historicalnodecount[0]}"
			echo "replicationfactor=$replicationfactor"
			echo "percentreplicate=${percentreplicate[0]}"
		} > "$2"/querysegmentandsizedistribution/"$segmentdistribution"/"$sizedistribution"/tmp_"$segmentdistribution"_"$sizedistribution".conf

		python DynamicMain.py "$2"/querysegmentandsizedistribution/"$segmentdistribution"/"$sizedistribution"/tmp_"$segmentdistribution"_"$sizedistribution".conf > "$2"/querysegmentandsizedistribution/"$segmentdistribution"/"$sizedistribution"/run_"$segmentdistribution"_"$sizedistribution".log
	done
done

wait

echo "Completed"
