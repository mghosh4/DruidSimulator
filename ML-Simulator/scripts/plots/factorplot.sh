#!/bin/sh
gnuplot << EOF
set terminal pngcairo font 'Times,20' linewidth 2 size 15in,9in 
set output "$2"
 
set xlabel "Time"
set ylabel "Number of Replicas"
set style line 1 lt rgb "#A00000" pt 1 pi 10 lw 2 ps 2
set style line 2 lt rgb "#00A000" pt 7 pi 10 lw 2 ps 2
set style line 3 lt rgb "#5060D0" pt 2 pi 10 lw 2 ps 2
set style line 4 lt rgb "#F25900" pt 5 pi 10 lw 2 ps 2
set style line 5 lt rgb "#ED0CCB" pt 9 pi 10 lw 2 ps 2
set autoscale                        # scale axes automatically
set xtic auto                        # set xtics automatically
set ytic auto                        # set ytics automatically
set xrange [0:1000]
set yrange [0:3]
set key top left
plot    "$1" using 1:2 title 'Fixed' with linespoints ls 1, \
	'' using 1:3 title 'Tiered (HT:300 WT:800)' with linespoints ls 2, \
	'' using 1:4 title 'Tiered (HT:100 WT:500)' with linespoints ls 3, \
        '' using 1:5 title 'Adaptive' with linespoints ls 4, \
        '' using 1:6 title 'Best Fit' with linespoints ls 5
EOF
