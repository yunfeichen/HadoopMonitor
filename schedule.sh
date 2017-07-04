#!/bin/bash
export APPPATH=/home/hadoop
export PATH=$APPPATH:$PATH:.

while true
do
  $APPPATH/getdata1.py >> $APPPATH/getdata.log
  /usr/local/zabbix/bin/zabbix_sender -vv -z 127.0.0.1 -i $APPPATH/getdata.data > $APPPATH/getdata.log
  sleep 10
done
