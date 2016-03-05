#!/bin/bash

fixed=($(grep $1 $2 | grep fixed | awk '{print $6}'))
tiered=($(grep $1 $2 | grep tiered | awk '{print $6}'))
adaptive=($(grep $1 $2 | grep adaptive | awk '{print $6}'))
bestfitd=($(grep $1 $2 | grep bestfit-d | awk '{print $6}'))

count=${#fixed[@]}
for ((i = 0; i < $count; i++))
do
	xaxis=$((($i + 1) * 10))
	if [ "$1" = "Replicas" ]; then
		improvement=$((${tiered[$i]} - ${adaptive[$i]}))
		improvementpercent=$(($improvement * 100 / ${tiered[$i]}))
		echo $xaxis ${fixed[$i]} ${tiered[$i]} ${adaptive[$i]} ${bestfitd[$i]} $improvementpercent
	else
		echo $xaxis ${fixed[$i]} ${tiered[$i]} ${adaptive[$i]} ${bestfitd[$i]}
	fi
done
