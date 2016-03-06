#!/bin/sh
gnuplot << EOF
set terminal pngcairo font 'Times,20' linewidth 2 size 15in,9in 
set output "$2"

set style data histograms
set style fill pattern 1 border lt -1
set ylabel "Throughput"
set style line 1 lt rgb "#A00000" pt 1 pi 10 lw 2 ps 2
set style line 2 lt rgb "#00A000" pt 7 pi 10 lw 2 ps 2
set style line 3 lt rgb "#5060D0" pt 2 pi 10 lw 2 ps 2
set style line 4 lt rgb "#F25900" pt 5 pi 10 lw 2 ps 2
set style line 5 lt rgb "#ED0CCB" pt 9 pi 10 lw 2 ps 2
set autoscale                        # scale axes automatically
unset xtics
set ytic auto                        # set ytics automatically
set yrange [0:10]
plot    "$1" using 2 ti 'Fixed', \
        '' using 3 ti 'Tiered', \
        '' using 4 ti 'Adaptive', \
        '' using 5 ti 'Best Fit'
EOF
