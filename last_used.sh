#!/usr/bin/env bash

for a in *ibd
do
	TS_TAB=`ls -l --time-style="+%s" /var/lib/mysql/ips/$a | awk '{print $6}'`
	TS_NOW=`date +%s`
	(( TS_DIFF = TS_NOW - TS_TAB ))
    printf "%05d (Time) Table: %s \n" "$TS_DIFF" "$a"
done | sort -k 1,4 -r
