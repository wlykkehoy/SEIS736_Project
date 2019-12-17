#!/usr/bin/env bash


echo WARNING: This will overwrite everything in the local VM directory \~/SparkProjects/WHVisitors/SparkOut
read -n 1 -p "Are you sure you wish to continue [y/n]? " resp
echo
if [[ $resp == "y" || $resp == "Y" ]]; then
	rm -r ~/SparkProjects/WHVisitors/SparkOut
	scp -r lykk3260@hc.gps.stthomas.edu:~/SparkProjects/WHVisitors/SparkOut ~/SparkProjects/WHVisitors/SparkOut
else
    echo "$0 : Aborted"
    exit 1
fi

exit 0

