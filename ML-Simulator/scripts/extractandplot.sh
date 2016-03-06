#!/bin/bash
RUNLOG_PATH=$1
LOG_PATH=$2
bash scripts/grepdata.sh Replicas $RUNLOG_PATH > "$LOG_PATH"/replicas.log
bash scripts/grepdata.sh Throughput $RUNLOG_PATH | tail -1 > "$LOG_PATH"/throughput.log
bash scripts/grepdata.sh Factor $RUNLOG_PATH > "$LOG_PATH"/factor.log
bash scripts/grepdata.sh Loads $RUNLOG_PATH | tail -1 > "$LOG_PATH"/loads.log
bash scripts/grepdata.sh Completion $RUNLOG_PATH | tail -1 > "$LOG_PATH"/completion.log

bash scripts/plots/replicaplot.sh $LOG_PATH/replicas.log $LOG_PATH/replicas.png
bash scripts/plots/throughput.sh $LOG_PATH/throughput.log $LOG_PATH/throughput.png
bash scripts/plots/factorplot.sh $LOG_PATH/factor.log $LOG_PATH/factor.png
bash scripts/plots/loads.sh $LOG_PATH/loads.log $LOG_PATH/loads.png
bash scripts/plots/completion.sh $LOG_PATH/completion.log $LOG_PATH/completion.png
