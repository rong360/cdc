#!/bin/bash
instance=""
master=""
javaPath=""
logBaseHome=""
while getopts "c:hi:j:l:" opt
do
    case $opt in
        c)master=$OPTARG;;
        i)instance=$OPTARG;;
        l)logBaseHome=$OPTARG;;
        j)javaPath=$OPTARG;;
        ?)
        echo "OPTIONS:"
        echo "	-i specify instance e.g. -i instance_a,instance_b "
        echo "	-c specify cluster default master (not required) e.g. -c beijing"
        echo "	-j specify java bin, if you do not configure the java environment variable needs to be specified -j /usr/bin/java"
        echo "	-l log4j base home e.g. -l /home/log/"
        echo "example: "
        echo "	sh start.sh -i instance_a,instance_b"
        exit 1;;
    esac
done

IFS=, DIRS=($instance)
if  [ ! -n "$instance" ] ;then
    echo "specify instance e.g. -i instance_a,instance_b"
	exit
fi

javaBin=`which java`
result=$(echo $javaBin | grep "no java in")
if [[ "$result" != "" ]] ;then
    if  [ ! -n "$javaPath" ] ;then
        echo "specify  java bin e.g. -j /usr/bin/java"
        exit
    else
        javaBin=$javaPath
    fi
fi

JAVA_OPTS=""
if  [ -n "$logBaseHome" ] ;then
    JAVA_OPTS="-Dcdc.log.base.home="$logBaseHome
fi

for file in ${DIRS[@]}
  do
		pid=`ps aux | grep com.rong360.main.Rong360CDC |grep "$file" |grep "$master"| grep -v grep | awk  '{print $2}'`
		if [ -z $pid ];then
			echo "starting "$master" cluster's instance:"$file
			nohup $javaBin -Xms1024m -Xmx1024m -Xmn512m -Djava.awt.headless=true $JAVA_OPTS com.rong360.main.Rong360CDC $file $master>/dev/null 2>&1 &
			#check start status
			sleep 5
			pid=`ps aux | grep com.rong360.main.Rong360CDC |grep "$file" |grep "$master"| grep -v grep | awk  '{print $2}'`
			if [ -z $pid ];then
				echo "start fail!"$master" cluster's instance:"$file
				echo "please check java env or etcd config:"$javaPath
			else
				echo "start success!"$master" cluster's instance:"$file" [pid:"$pid"]"
			fi
		else
			echo "already start success!"$master" cluster's instance:"$file" [pid:"$pid"]"
		fi
done
echo "done"