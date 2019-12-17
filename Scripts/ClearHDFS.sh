#!/usr/bin/env bash


if [ $# -ne 2 ]; then
    echo "Usage : $0 dataset [Data | SparkOut]"
    exit 1
fi


if [[ "$1" != "VSmall" && "$1" != "OneLog" && "$1" != "Full" ]]; then
	echo "$0 : ERROR: Unknow dataset $1"
	exit 1
fi


case "$2" in
Data)
	dir_to_blast=SparkProjects/WHVisitors/Data/$1
	;;
SparkOut)
	dir_to_blast=SparkProjects/WHVisitors/SparkOut/$1
	;;
*)
	echo "$0 : ERROR : Second parameter must be either Data or SparkOut"
	exit 1
	;;
esac


echo WARNING: This will delete the HDFS directory ${dir_to_blast}
read -n 1 -p "Are you sure you wish to continue [y/n]? " resp
echo
if [[ $resp == "y" || $resp == "Y" ]]; then
	hdfs dfs -rm -r ${dir_to_blast}
else
    echo "$0 : Aborted"
	exit 1
fi

exit 0


