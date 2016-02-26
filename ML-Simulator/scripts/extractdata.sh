#!/bin/bash

for segment in 'latest' 'uniform' 'zipfian'
do
	for size in 'zipfian' 'uniform'
	do
		echo $segment $size
		fixed=($(grep Replicas experiments/logs/querysegmentandsizedistribution/$segment/$size/run_"$segment"_"$size".log | grep fixed | awk '{print $7}'))
		tiered=($(grep Replicas experiments/logs/querysegmentandsizedistribution/$segment/$size/run_"$segment"_"$size".log | grep tiered | awk '{print $7}'))
		adaptive=($(grep Replicas experiments/logs/querysegmentandsizedistribution/$segment/$size/run_"$segment"_"$size".log | grep adaptive | awk '{print $7}'))
		bestfitd=($(grep Replicas experiments/logs/querysegmentandsizedistribution/$segment/$size/run_"$segment"_"$size".log | grep bestfit-d | awk '{print $7}'))
		bestfits=`grep Replicas experiments/logs/querysegmentandsizedistribution/$segment/$size/run_"$segment"_"$size".log | grep bestfit-s | awk '{print $7}'`
		time=`grep Time experiments/logs/querysegmentandsizedistribution/$segment/$size/run_"$segment"_"$size".log | tail -5 | awk '{print $6}'`

		count=${#fixed[@]}
		for ((i = 0; i < $count; i++))
		do
			echo ${fixed[$i]} ${tiered[$i]} ${adaptive[$i]} ${bestfitd[$i]}
		done
		echo "$bestfits"
		echo "$time"
	done
done
