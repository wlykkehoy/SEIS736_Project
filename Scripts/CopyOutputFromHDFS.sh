#!/usr/bin/env bash


if [ $# -ne 2 ]; then
    echo "Usage : $0 dataset [<process> | All]"
    exit 1
fi


if [[ "$1" == "VSmall"  || "$1" == "OneLog" || "$1" == "Full" ]]; then
	case "$2" in
	All)
		rm -r ~/SparkProjects/WHVisitors/SparkOut/$1
		hdfs dfs -get SparkProjects/WHVisitors/SparkOut/$1 ~/SparkProjects/WHVisitors/SparkOut/$1
		ls -l ~/SparkProjects/WHVisitors/SparkOut/$1 | more
		;;
	Cleanse | GenStats | XrefContacts)
		rm -r ~/SparkProjects/WHVisitors/SparkOut/$1/$2
		hdfs dfs -get SparkProjects/WHVisitors/SparkOut/$1/$2 ~/SparkProjects/WHVisitors/SparkOut/$1/$2
		ls -l ~/SparkProjects/WHVisitors/SparkOut/$1/$2 | more
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



