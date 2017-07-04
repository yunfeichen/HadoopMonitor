#!/usr/bin/python
# -*- coding:utf-8 -*-
# File: collectdata.py
import urllib2
import json
import os

#settings section
ZABBIX_NAME="namenode"
CLUSTER_HOST="10.199.207.5"
GETDATAFILE="/home/hadoop/getdata.data"
GETDATALOG="/home/hadoop/getdata.log"

#--------------------------------------------------------------------------------------------
# HeapMemory
#--------------------------------------------------------------------------------------------
url1 = "http://"+CLUSTER_HOST+":50070/jmx?qry=java.lang:type=Memory"
response = urllib2.Request(url1)
res_data = urllib2.urlopen(response)
res = res_data.read()
hjson=json.loads(res)
heap_memory_committed=round(float(hjson['beans'][0]["HeapMemoryUsage"]["committed"])/1024/1024,2)
heap_memory_init=round(float(hjson['beans'][0]["HeapMemoryUsage"]["init"])/1024/1024,2)
heap_memory_max=round(float(hjson['beans'][0]["HeapMemoryUsage"]["max"])/1024/1024,2)
heap_memory_used=round(float(hjson['beans'][0]["HeapMemoryUsage"]["used"])/1024/1024,2)
nonheap_memory_committed=round(float(hjson['beans'][0]["NonHeapMemoryUsage"]["committed"])/1024/1024,2)
nonheap_memory_init=round(float(hjson['beans'][0]["NonHeapMemoryUsage"]["init"])/1024/1024,2)
nonheap_memory_max=round(float(hjson['beans'][0]["NonHeapMemoryUsage"]["max"])/1024/1024,2)
nonheap_memory_used=round(float(hjson['beans'][0]["NonHeapMemoryUsage"]["used"])/1024/1024,2)

#--------------------------------------------------------------------------------------------
# FSNamesystemState
#--------------------------------------------------------------------------------------------
url2 = "http://"+CLUSTER_HOST+":50070/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
response = urllib2.Request(url2)
res_data = urllib2.urlopen(response)
res = res_data.read()
hjson=json.loads(res)

live_nodes=hjson['beans'][0]["NumLiveDataNodes"]
dead_nodes=hjson['beans'][0]["NumDeadDataNodes"]
decom_live_nodes=hjson['beans'][0]["NumDecomLiveDataNodes"]
decom_dead_nodes=hjson['beans'][0]["NumDecomDeadDataNodes"]
volume_failures_total=hjson['beans'][0]["VolumeFailuresTotal"]
estimated_capacitylost_total=hjson['beans'][0]["EstimatedCapacityLostTotal"]
decommissioning_nodes=hjson['beans'][0]["NumDecommissioningDataNodes"]
pending_repllicated_blocks=hjson['beans'][0]["PendingReplicationBlocks"]
under_repllicated_blocks=hjson['beans'][0]["UnderReplicatedBlocks"]
scheduled_repllicated_blocks=hjson['beans'][0]["ScheduledReplicationBlocks"]
pending_deletion_blocks=hjson['beans'][0]["PendingDeletionBlocks"]

#--------------------------------------------------------------------------------------------
# NameNodeInfo
#--------------------------------------------------------------------------------------------
url1 = "http://"+CLUSTER_HOST+":50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
response = urllib2.Request(url1)
res_data = urllib2.urlopen(response)
res = res_data.read()
hjson=json.loads(res)
start_time=hjson['beans'][0]["NNStarted"]
hadoop_version=hjson['beans'][0]["SoftwareVersion"]
file_and_directory_count=hjson['beans'][0]["TotalFiles"]
dfs_blocks=hjson['beans'][0]["TotalBlocks"]
storage_unit="TB"
configured_cluster_storage=hjson['beans'][0]["Total"]
configured_cluster_storage=round(float(configured_cluster_storage)/1024/1024/1024/1024,2)
dfs_use_storage=hjson['beans'][0]["Used"]
dfs_use_storage=round(float(dfs_use_storage)/1024/1024/1024/1024,2)
non_dfs_use_storage=hjson['beans'][0]["NonDfsUsedSpace"]
non_dfs_use_storage=round(float(non_dfs_use_storage)/1024/1024/1024/1024,2)
available_dfs_storage=hjson['beans'][0]["Free"]
available_dfs_storage=round(float(available_dfs_storage)/1024/1024/1024/1024,2)
used_storage_pct=hjson['beans'][0]["PercentUsed"]
used_storage_pct=round(float(used_storage_pct),2)
available_storage_pct=hjson['beans'][0]["PercentRemaining"]
available_storage_pct=round(float(available_storage_pct),2)

test=hjson['beans'][0]["LiveNodes"]
test1=test.replace("\\","")
hjson1=json.loads(test1)

max_configured_storage_node_name = ""
max_configured_storage = 0
max_used_storage_node_name = ""
max_used_storage = 0
max_non_dfs_used_storage_node_name = ""
max_non_dfs_used_storage = 0
max_free_storage_node_name = ""
max_free_storage = 0
max_used_storage_pct_node_name = ""
max_used_storage_pct = 0
max_free_storage_pct_node_name = ""
max_free_storage_pct = 0


for key in hjson1:
    if (isinstance(hjson1[key], dict)):
        if hjson1[key]["capacity"]>max_configured_storage:
            max_configured_storage=hjson1[key]["capacity"]
            max_configured_storage_node_name=key
        if hjson1[key]["used"]>max_used_storage:
            max_used_storage=hjson1[key]["used"]
            max_used_storage_node_name=key
        if hjson1[key]["nonDfsUsedSpace"]>max_non_dfs_used_storage:
            max_non_dfs_used_storage=hjson1[key]["nonDfsUsedSpace"]
            max_non_dfs_used_storage_node_name=key
        if hjson1[key]["remaining"]>max_free_storage:
            max_free_storage=hjson1[key]["remaining"]
            max_free_storage_node_name=key
        if hjson1[key]["used"]*100/hjson1[key]["capacity"]>max_used_storage_pct:
            max_used_storage_pct=round(float(hjson1[key]["used"]*100)/hjson1[key]["capacity"],2)
            max_used_storage_pct_node_name=key
        if hjson1[key]["remaining"]*100/hjson1[key]["capacity"]>max_free_storage_pct:
            max_free_storage_pct=round(float(hjson1[key]["remaining"]*100)/hjson1[key]["capacity"],2)
            max_free_storage_pct_node_name=key

min_configured_storage_node_name = max_configured_storage_node_name
min_configured_storage = max_configured_storage
min_used_storage_node_name = ""
min_used_storage = max_used_storage
min_non_dfs_used_storage_node_name = ""
min_non_dfs_used_storage = max_non_dfs_used_storage
min_free_storage_node_name = ""
min_free_storage = max_free_storage
min_used_storage_pct_node_name = max_used_storage_pct_node_name
min_used_storage_pct = 100
min_free_storage_pct_node_name = max_free_storage_pct_node_name
min_free_storage_pct = 100
for key in hjson1:
    if (isinstance(hjson1[key], dict)):
        if hjson1[key]["capacity"]<min_configured_storage:
            min_configured_storage=hjson1[key]["capacity"]
            min_configured_storage_node_name=key
        if hjson1[key]["used"]<min_used_storage:
            min_used_storage=hjson1[key]["used"]
            min_used_storage_node_name=key
        if hjson1[key]["nonDfsUsedSpace"]<min_non_dfs_used_storage:
            min_non_dfs_used_storage=hjson1[key]["nonDfsUsedSpace"]
            min_non_dfs_used_storage_node_name=key
        if hjson1[key]["remaining"]<min_free_storage:
            min_free_storage=hjson1[key]["remaining"]
            min_free_storage_node_name=key
        if hjson1[key]["used"]*100/hjson1[key]["capacity"]<min_used_storage_pct:
            min_used_storage_pct=round(float(hjson1[key]["used"]*100)/hjson1[key]["capacity"],2)
            min_used_storage_pct_node_name=key
        if hjson1[key]["remaining"]*100/hjson1[key]["capacity"]<min_free_storage_pct:
            min_free_storage_pct=round(float(hjson1[key]["remaining"]*100)/hjson1[key]["capacity"],2)
            min_free_storage_pct_node_name=key

max_configured_storage=round(float(max_configured_storage)/1024/1024/1024/1024,2)
max_used_storage=round(float(max_used_storage)/1024/1024/1024/1024,2)
max_non_dfs_used_storage=round(float(max_non_dfs_used_storage)/1024/1024/1024/1024,2)
max_free_storage=round(float(max_free_storage)/1024/1024/1024/1024,2)
max_used_storage_pct=round(float(max_used_storage_pct),2)
max_free_storage_pct=round(float(max_free_storage_pct),2)

min_configured_storage=round(float(min_configured_storage)/1024/1024/1024/1024,2)
min_used_storage=round(float(min_used_storage)/1024/1024/1024/1024,2)
min_non_dfs_used_storage=round(float(min_non_dfs_used_storage)/1024/1024/1024/1024,2)
min_free_storage=round(float(min_free_storage)/1024/1024/1024/1024,2)
min_used_storage_pct=round(float(min_used_storage_pct),2)
min_free_storage_pct=round(float(min_free_storage_pct),2)

#--------------------------------------------------------------------------------------------
# Resource Manager
#--------------------------------------------------------------------------------------------
url1 = "http://"+CLUSTER_HOST+":8088/jmx?qry=Hadoop:service=ResourceManager,name=ClusterMetrics"
response = urllib2.Request(url1)
res_data = urllib2.urlopen(response)
res = res_data.read()
hjson=json.loads(res)

num_active_nms=hjson['beans'][0]["NumActiveNMs"]
num_decommissioned_nms=hjson['beans'][0]["NumDecommissionedNMs"]
num_lost_nms=hjson['beans'][0]["NumLostNMs"]
num_unhealthy_nms=hjson['beans'][0]["NumUnhealthyNMs"]
num_rebooted_nms=hjson['beans'][0]["NumRebootedNMs"]

url1 = "http://"+CLUSTER_HOST+":8088/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root"
response = urllib2.Request(url1)
res_data = urllib2.urlopen(response)
res = res_data.read()
hjson=json.loads(res)

running_0=hjson['beans'][0]["running_0"]
running_60=hjson['beans'][0]["running_60"]
running_300=hjson['beans'][0]["running_300"]
running_1440=hjson['beans'][0]["running_1440"]
apps_submitted=hjson['beans'][0]["AppsSubmitted"]
apps_running=hjson['beans'][0]["AppsRunning"]
apps_pending=hjson['beans'][0]["AppsPending"]
apps_completed=hjson['beans'][0]["AppsCompleted"]
apps_killed=hjson['beans'][0]["AppsKilled"]
apps_failed=hjson['beans'][0]["AppsFailed"]
allocated_mb=hjson['beans'][0]["AllocatedMB"]/1024
allocated_vcores=hjson['beans'][0]["AllocatedVCores"]
allocated_containers=hjson['beans'][0]["AllocatedContainers"]
aggregate_containers_allocated=hjson['beans'][0]["AggregateContainersAllocated"]
avaliable_mb=hjson['beans'][0]["AvailableMB"]/1024
avaliable_vcores=hjson['beans'][0]["AvailableVCores"]
pending_mb=hjson['beans'][0]["PendingMB"]/1024
pending_vcores=hjson['beans'][0]["PendingVCores"]
pending_containers=hjson['beans'][0]["PendingContainers"]
reserved_mb=hjson['beans'][0]["ReservedMB"]/1024
reserved_vcores=hjson['beans'][0]["ReservedVCores"]
reserved_containers=hjson['beans'][0]["ReservedContainers"]
active_users=hjson['beans'][0]["ActiveUsers"]
active_applications=hjson['beans'][0]["ActiveApplications"]

f = open(GETDATAFILE, 'w+')
print >> f,ZABBIX_NAME,"heap_memory_committed" ,heap_memory_committed
print >> f,ZABBIX_NAME,"heap_memory_init" ,heap_memory_init
print >> f,ZABBIX_NAME,"heap_memory_max" ,heap_memory_max
print >> f,ZABBIX_NAME,"heap_memory_used" ,heap_memory_used
print >> f,ZABBIX_NAME,"nonheap_memory_committed" ,nonheap_memory_committed
print >> f,ZABBIX_NAME,"nonheap_memory_init" ,nonheap_memory_init
print >> f,ZABBIX_NAME,"nonheap_memory_max" ,nonheap_memory_max
print >> f,ZABBIX_NAME,"nonheap_memory_used" ,nonheap_memory_used

print >> f,ZABBIX_NAME,"start_time" ,start_time
print >> f,ZABBIX_NAME,"hadoop_version" ,hadoop_version
print >> f,ZABBIX_NAME,"file_and_directory_count" ,file_and_directory_count
print >> f,ZABBIX_NAME,"dfs_blocks" ,dfs_blocks
print >> f,ZABBIX_NAME,"storage_unit" ,storage_unit
print >> f,ZABBIX_NAME,"configured_cluster_storage" ,configured_cluster_storage
print >> f,ZABBIX_NAME,"dfs_use_storage" ,dfs_use_storage
print >> f,ZABBIX_NAME,"non_dfs_use_storage" ,non_dfs_use_storage
print >> f,ZABBIX_NAME,"available_dfs_storage" ,available_dfs_storage
print >> f,ZABBIX_NAME,"used_storage_pct" ,used_storage_pct
print >> f,ZABBIX_NAME,"available_storage_pct" ,available_storage_pct
print >> f,ZABBIX_NAME,"live_nodes" ,live_nodes
print >> f,ZABBIX_NAME,"dead_nodes" ,dead_nodes
print >> f,ZABBIX_NAME,"decom_live_nodes" ,decom_live_nodes
print >> f,ZABBIX_NAME,"decom_dead_nodes" ,decom_dead_nodes
print >> f,ZABBIX_NAME,"volume_failures_total" ,volume_failures_total
print >> f,ZABBIX_NAME,"estimated_capacitylost_total" ,estimated_capacitylost_total
print >> f,ZABBIX_NAME,"decommissioning_nodes" ,decommissioning_nodes
print >> f,ZABBIX_NAME,"pending_repllicated_blocks" ,pending_repllicated_blocks
print >> f,ZABBIX_NAME,"under_repllicated_blocks" ,under_repllicated_blocks
print >> f,ZABBIX_NAME,"scheduled_repllicated_blocks" ,scheduled_repllicated_blocks
print >> f,ZABBIX_NAME,"pending_deletion_blocks" ,pending_deletion_blocks

print >> f,ZABBIX_NAME,"max_configured_storage_node_name",max_configured_storage_node_name
print >> f,ZABBIX_NAME,"max_configured_storage",max_configured_storage
print >> f,ZABBIX_NAME,"max_used_storage_node_name",max_used_storage_node_name
print >> f,ZABBIX_NAME,"max_used_storage",max_used_storage
print >> f,ZABBIX_NAME,"max_non_dfs_used_storage_node_name",max_non_dfs_used_storage_node_name
print >> f,ZABBIX_NAME,"max_non_dfs_used_storage",max_non_dfs_used_storage
print >> f,ZABBIX_NAME,"max_free_storage_node_name",max_free_storage_node_name
print >> f,ZABBIX_NAME,"max_free_storage",max_free_storage
print >> f,ZABBIX_NAME,"max_used_storage_pct_node_name",max_used_storage_pct_node_name
print >> f,ZABBIX_NAME,"max_used_storage_pct",max_used_storage_pct
print >> f,ZABBIX_NAME,"max_free_storage_pct_node_name",max_free_storage_pct_node_name
print >> f,ZABBIX_NAME,"max_free_storage_pct",max_free_storage_pct

print >> f,ZABBIX_NAME,"min_configured_storage_node_name",min_configured_storage_node_name
print >> f,ZABBIX_NAME,"min_configured_storage",min_configured_storage
print >> f,ZABBIX_NAME,"min_used_storage_node_name",min_used_storage_node_name
print >> f,ZABBIX_NAME,"min_used_storage",min_used_storage
print >> f,ZABBIX_NAME,"min_non_dfs_used_storage_node_name",min_non_dfs_used_storage_node_name
print >> f,ZABBIX_NAME,"min_non_dfs_used_storage",min_non_dfs_used_storage
print >> f,ZABBIX_NAME,"min_free_storage_node_name",min_free_storage_node_name
print >> f,ZABBIX_NAME,"min_free_storage",min_free_storage
print >> f,ZABBIX_NAME,"min_used_storage_pct_node_name",min_used_storage_pct_node_name
print >> f,ZABBIX_NAME,"min_used_storage_pct",min_used_storage_pct
print >> f,ZABBIX_NAME,"min_free_storage_pct_node_name",min_free_storage_pct_node_name
print >> f,ZABBIX_NAME,"min_free_storage_pct",min_free_storage_pct


print >> f,ZABBIX_NAME,"num_active_nms" ,num_active_nms
print >> f,ZABBIX_NAME,"num_decommissioned_nms" ,num_decommissioned_nms
print >> f,ZABBIX_NAME,"num_lost_nms" ,num_lost_nms
print >> f,ZABBIX_NAME,"num_unhealthy_nms" ,num_unhealthy_nms
print >> f,ZABBIX_NAME,"num_rebooted_nms" ,num_rebooted_nms

print >> f,ZABBIX_NAME,"running_0" ,running_0
print >> f,ZABBIX_NAME,"running_60" ,running_60
print >> f,ZABBIX_NAME,"running_300" ,running_300
print >> f,ZABBIX_NAME,"running_1440" ,running_1440
print >> f,ZABBIX_NAME,"apps_submitted" ,apps_submitted
print >> f,ZABBIX_NAME,"apps_running" ,apps_running
print >> f,ZABBIX_NAME,"apps_pending" ,apps_pending
print >> f,ZABBIX_NAME,"apps_completed" ,apps_completed
print >> f,ZABBIX_NAME,"apps_killed" ,apps_killed
print >> f,ZABBIX_NAME,"apps_failed" ,apps_failed
print >> f,ZABBIX_NAME,"allocated_mb" ,allocated_mb
print >> f,ZABBIX_NAME,"allocated_vcores" ,allocated_vcores
print >> f,ZABBIX_NAME,"allocated_containers" ,allocated_containers
print >> f,ZABBIX_NAME,"aggregate_containers_allocated" ,aggregate_containers_allocated
print >> f,ZABBIX_NAME,"avaliable_mb" ,avaliable_mb
print >> f,ZABBIX_NAME,"avaliable_vcores" ,avaliable_vcores
print >> f,ZABBIX_NAME,"pending_mb" ,pending_mb
print >> f,ZABBIX_NAME,"pending_vcores" ,pending_vcores
print >> f,ZABBIX_NAME,"pending_containers" ,pending_containers
print >> f,ZABBIX_NAME,"reserved_mb" ,reserved_mb
print >> f,ZABBIX_NAME,"reserved_vcores" ,reserved_vcores
print >> f,ZABBIX_NAME,"reserved_containers" ,reserved_containers
print >> f,ZABBIX_NAME,"active_users" ,active_users
print >> f,ZABBIX_NAME,"active_applications" ,active_applications

