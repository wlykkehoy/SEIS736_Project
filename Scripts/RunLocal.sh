#!/usr/bin/env bash

if [ $# -ne 2 ]
then
	echo "Usage : $0 dataset process"
	exit 1
fi


if [[ "$1" == "VSmall"  || "$1" == "OneLog" || "$1" == "Full" ]]; then
	case "$2" in
	Cleanse)
		hdfs dfs -rm -r SparkProjects/WHVisitors/Data/$1/CleansedLogs
		hdfs dfs -rm -r SparkProjects/WHVisitors/SparkOut/$1/Cleanse

		spark-submit \
		  --class WHVisitors_Cleanse \
		  --master local[*] \
		  --name "${USER}-WHVisitors_Cleanse" \
		  ~/SparkProjects/WHVisitors/WHVisitors_Cleanse.jar \
		  SparkProjects/WHVisitors/Data/$1/RawLogs \
		  SparkProjects/WHVisitors/Data/$1/CleansedLogs \
		  SparkProjects/WHVisitors/SparkOut/$1/Cleanse

		hdfs dfs -ls SparkProjects/WHVisitors/Data/$1/CleansedLogs
		hdfs dfs -ls SparkProjects/WHVisitors/SparkOut/$1/Cleanse
		;;
	GenStats)
		hdfs dfs -rm -r SparkProjects/WHVisitors/SparkOut/$1/GenStats

		spark-submit \
		  --class WHVisitors_GenStats \
		  --master local[*] \
		  --name "${USER}-WHVisitors_GenStats" \
		  ~/SparkProjects/WHVisitors/WHVisitors_GenStats.jar \
		  SparkProjects/WHVisitors/Data/$1/CleansedLogs \
		  SparkProjects/WHVisitors/SparkOut/$1/GenStats

		hdfs dfs -ls SparkProjects/WHVisitors/SparkOut/$1/GenStats
		;;
	XrefContacts)
		hdfs dfs -rm -r SparkProjects/WHVisitors/SparkOut/$1/XrefContacts

		spark-submit \
		  --class WHVisitors_XrefContacts \
		  --master local[*] \
		  --name "${USER}-WHVisitors_XrefContacts" \
		  ~/SparkProjects/WHVisitors/WHVisitors_XrefContacts.jar \
		  SparkProjects/WHVisitors/Data/$1/CleansedLogs \
		  SparkProjects/WHVisitors/Data/$1/Contacts \
		  SparkProjects/WHVisitors/SparkOut/$1/XrefContacts

		hdfs dfs -ls SparkProjects/WHVisitors/SparkOut/$1/XrefContacts
		;;
	*)
		echo "$0 : ERROR : Unknown process $2"
		exit 1
		;;
	esac
else
	echo "$0 : ERROR : Unknown dataset $1"
	exit 1
fi

exit 0
