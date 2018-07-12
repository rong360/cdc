#!/bin/bash
instance=""
master=""
all=""
while getopts "c:hai:" opt
do
    case $opt in
        c)master=$OPTARG;;
        i)instance=$OPTARG;;
        a)all="1";;
        ?)
        echo "OPTIONS:"
        echo "	-i specify instance e.g. -i instance_a,instance_b "
        echo "	-c specify cluster default master (not required)"
        echo "  -a stop all cdc service"
        echo "example: "
        echo "	sh stop.sh -i instance_a,instance_b"
        exit 1;;
    esac
done
fileext="$"
if [  -n "$master" ] ;then
    fileext=" "
fi
IFS=, DIRS=($instance)
if  [ -n "$all" ] ;then
    echo "stopping all cdc service"
    ps -ef | grep com.rong360.main.Rong360CDC | grep -v grep | awk  '{print $2}' | xargs kill -15
    sleep 1
elif [ ! -n "$instance" ] ;then
    echo "specify instance e.g. -i instance_a,instance_b or -a"
	exit
fi
for file in ${DIRS[@]}
  do
  	echo "stopping "$master" cluster's instance:"$file
    ps -ef | grep com.rong360.main.Rong360CDC | grep "$file$fileext"|grep "$master$" | grep -v grep | awk  '{print $2}' | xargs kill -15
    sleep 1
  done

