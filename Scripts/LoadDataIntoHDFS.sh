#!/usr/bin/env bash


if [ $# -ne 2 ]; then
	echo "Usage : $0 dataset which_data"
	exit 1
fi


if [[ "$1" == "VSmall"  || "$1" == "OneLog" || "$1" == "Full" ]]; then
	if [[ "$2" == "RawLogs" || "$2" == "CleansedLogs" || "$2" == "Contacts" ]]; then
		hdfs dfs -mkdir -p SparkProjects/WHVisitors/Data/$1
		hdfs dfs -rm -r SparkProjects/WHVisitors/Data/$1/$2
		hdfs dfs -put ~/SparkProjects/WHVisitors/Data/$1/$2 SparkProjects/WHVisitors/Data/$1
		hdfs dfs -ls SparkProjects/WHVisitors/Data/$1
	elif [[ "$2" == "CleansedLogs" ]]; then
		echo "$0 : ERROR : Unknown datasource $2"
		exit 1
	fi
else
	echo "$0 : ERROR : Unknown dataset $1"
	exit 1
fi

exit 0
