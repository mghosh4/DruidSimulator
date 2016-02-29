#!/bin/bash

fixed=($(grep $2 $1 | grep fixed | awk '{print $6}'))
tiered=($(grep $2 $1 | grep tiered | awk '{print $6}'))
adaptive=($(grep $2 $1 | grep adaptive | awk '{print $6}'))
bestfitd=($(grep $2 $1 | grep bestfit-d | awk '{print $6}'))

count=${#fixed[@]}
for ((i = 0; i < $count; i++))
do
	echo ${fixed[$i]} ${tiered[$i]} ${adaptive[$i]} ${bestfitd[$i]}
done
