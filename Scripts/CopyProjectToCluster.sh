#!/usr/bin/env bash


if [ $# -ne 1 ]; then
    echo "Usage : $0 [jar | Data | Scripts]"
    exit 1
fi

case "$1" in
jar)
	message="WARNING: This will overwrite the ~/SparkProjects/WHVisitors/*.jar files on the cluster"
	;;
Data)
	message="WARNING: This will overwrite directory ~/SparkProjects/WHVisitors/Data on the cluster"
	;;
Scripts)
	message="WARNING: This will overwrite directory ~/SparkProjects/WHVisitors/Scripts on the cluster"
	;;
*)
echo "$0 : ERROR : Argument must be jar | Data | Scripts"
	exit 1
	;;
esac


echo ${message}
read -n 1 -p "Are you sure you wish to continue [y/n]? " resp
echo
if [[ $resp == "y" || $resp == "Y" ]]; then
	case "$1" in
	jar)
		ssh lykk3260@hc.gps.stthomas.edu rm -r SparkProjects/WHVisitors/*.jar
		scp -r ~/SparkProjects/WHVisitors/*.jar lykk3260@hc.gps.stthomas.edu:~/SparkProjects/WHVisitors
		;;
	Data)
		ssh lykk3260@hc.gps.stthomas.edu rm -r SparkProjects/WHVisitors/Data
		scp -r ~/SparkProjects/WHVisitors/Data lykk3260@hc.gps.stthomas.edu:~/SparkProjects/WHVisitors
		;;
	Scripts)
		ssh lykk3260@hc.gps.stthomas.edu rm -r SparkProjects/WHVisitors/Scripts
		scp -r ~/SparkProjects/WHVisitors/Scripts lykk3260@hc.gps.stthomas.edu:~/SparkProjects/WHVisitors
		;;
	*)
		echo "$0 : ERROR : Oops, this should not have happened, must be an error in the script"
		exit 1
		;;
	esac
else
    echo "$0 : Aborted"
    exit 1
fi

exit 0

