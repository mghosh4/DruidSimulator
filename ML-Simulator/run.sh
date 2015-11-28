#!/bin/bash

#echo "Varying Query Count"
#bash scripts/querycount.sh
echo "Varying Query Segment Distribution"
bash scripts/querysegmentdistribution.sh
echo "Varying Query Size Distribution"
bash scripts/querysizedistribution.sh
echo "Varying Historical Node Count"
bash scripts/historicalnodecount.sh
