#!/bin/bash

# get ips from './server_ip'
ip_file='./server_ip'
i=0
for ip in $(awk '{print $1}' $ip_file)
do
	ip_group[$i]="$ip"
	i=`expr $i + 1`
done

ip_total_num=$i

ip_num=$1
nd_num=$2


# determine ip_num
if [ "$ip_num" = "" ]; then
	echo "ip_num can not be NULL, set to be $ip_total_num"
fi


# determine nd_num
if [ "$nd_num" = "" ]; then
	echo 'nd_num can not be NULL. set it to 10'
	nd_num=10
fi


# create './cli-configure.json'
cli_conf='./cli-configure.json'
echo "[" > $cli_conf


REDIS_BASE_PORT=6380
THRIFT_BASE_PORT=9090

last_ip=${ip_group[${#ip_group[@]}-1]}

for nd_id in $(seq 1 $nd_num)
do
	# determine redis port and thrift port
	redis_port=`expr $REDIS_BASE_PORT + $nd_id`
	thrift_port=`expr $THRIFT_BASE_PORT + $nd_id`
	
	# generate json with jo cmd, ref: https://github.com/jpmens/jo/blob/master/jo.md
	for ip in ${ip_group[@]}
	do
		nd_obj=$(jo "ADDRESS"="$ip" "REDIS PORT"=$redis_port "THRIFT PORT"=$thrift_port)
		if [ $nd_id !=  $nd_num ]||[ "$ip" != "$last_ip" ]; then
			nd_obj=$nd_obj','
		fi
		echo $nd_obj >> $cli_conf
	done	
done


echo "]" >> $cli_conf
echo 'done.'
