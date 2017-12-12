#!/bin/bash -eu

# Compute an approximation of the definite integral
# of sqrt(x) from 0 to 1 using Monte Carlo method.
#
# $1: number of random points (default 1000)
# $2: (if distributed) sshlogin for -S, see parallel(1)

NUM_SAMPLES=${1:-1000}
SSH_PARAMS=${2:+"-S $2"}

SAMPLES=$(seq $NUM_SAMPLES)
RESULTS="$(parallel ${SSH_PARAMS} -n0 \
         "echo sqrt\(\$RANDOM/32767\) | bc -l" ::: $SAMPLES)"

printf "($RESULTS)/$NUM_SAMPLES" | paste -sd'+' | bc -l
