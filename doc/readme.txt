This tool migrates from a source hbase 0.94.12 based on hadoop V1 to a target hbase 1.1.1 based on hadoop V2.

This tool launches a map reduce that requires a tablewriter.jar file installed on HDFS. The properties "hdfs.default.name" and "jar" of environment.properties are used to locate the jar.

1) Installation:

1.1) The following steps describe how to copy that jar from a local FS to the source HDFS.

scp ~/workspace/tablewriter/target/tablewriter-1.2.4.jar despegar@hb-master-beta-00:/home/despegar
ssh("hb-master-beta",0)
cd /opt/hadoop/hadoop-1.0.4/
bin/hadoop fs -copyFromLocal ~/tablewriter-1.2.4.jar /tmp/tablewriter-1.2.4.jar

1.2) Install the migrator tool into the source Hbase.

scp ~/workspace/migrator/target/migrator-0.0.1.tar.gz despegar@vmAustria.servers.despegar.it:/opt
ssh vmAustria.servers.despegar.it
cd /opt
tar -zxvf migrator-0.0.1.tar.gz


Usage:

java -jar migrator-0.0.1.jar <environment.properties file path> <tables.properties file path> [run|test]

Example:

java -jar migrator-0.0.1.jar environment.properties tables.properties test

Where

test mode will dump all the properties and will check that the tablewriter.jar file exists in HDFS.
run mode will dump all the properties and perform the migration.

Two property files are needed to configure and select the tables to be migrated.

table.properties defines all the tables that will be migrated, of the form sourceTableName = targetTableName, for example:

segments=eu:segments
profiling=eu:profiling


environment.properties contains several configuration settings, for example we used the following for IC :

jar=/tmp/tablewriter-1.2.4.jar
hbase.client.retries.number=2
batch.size=1000					# number of rows to be inserted at once, using batch mode.
synchronic=true					# true migrate one table after other, sequentially. false submits one map reduce per table.
migrator.retry.attempts=50		# max retry attempts, useful to avoid split regions while migrating big tables.
migrator.retry.sleep=5000		# time to sleep in milliseconds between each retry.


# Source is IC H1
hbase.zookeeper.quorum=vmaustria.servers.despegar.it:2181
hdfs.default.name=hdfs://vmAustria.servers.despegar.it:9000
zookeeper.znode.parent=/hbase
mapred.job.tracker=vmAustria.servers.despegar.it:9001


# Target is IC H2
target.zk.quorum=zk-p13n-bsas-ic-00.servers.despegar.it
target.znode.parent=/p13n/ic/hbase
target.fs.name=hdfs://p13n-hb-ic-00.servers.despegar.it:8020
