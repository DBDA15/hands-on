#! /bin/bash


# setting necessary variabels

HADOOP_DIR=/root/ephemeral-hdfs
read -p "Please enter the path to Hadoop home (default: $HADOOP_DIR): " input;
HADOOP_DIR=${input:-$default}


SPARK_DIR=$(find "/root/spark" 2> /dev/null | grep "ec2/spark-ec2" | sed s:/ec2/spark-ec2$::g)
SPARK_DIR=${SPARK_DIR:-"/root/spark"}
read -p "Please enter the path to Spark home (default: $SPARK_DIR): " input;
SPARK_DIR=${input:-$SPARK_DIR}


FLINK_BINARY_URL="http://www.eu.apache.org/dist/flink/flink-0.9.0-milestone-1/flink-0.9.0-milestone-1-bin-hadoop2.tgz"
read -p "Please enter the URL to the Flink binaries (default: $FLINK_BINARY_URL): " input;
FLINK_BINARY_URL=${input:-$FLINK_BINARY_URL}


FLINK_DIR=/root/flink
read -p "Please enter the path to Spark home (default: $FLINK_DIR): " input;
FLINK_DIR=${input:-$FLINK_DIR}


echo Downloadng Flink binaries...
#cd /root
rm -f flink.tgz
wget -O flink.tgz $FLINK_BINARY_URL

echo extracing ...
rm -rf $FLINK_DIR && mkdir $FLINK_DIR
tmp="$FLINK_DIR/$(tar xzfv flink.tgz -C $FLINK_DIR/ | grep "conf/flink-conf.yaml" | sed s:/conf/flink-conf.yaml$::g)"
mv $tmp/* $FLINK_DIR && rm -rf $tmp 
rm -f flink.tgz

MASTER=`cat $SPARK_DIR-ec2/masters`
SLAVES=`wc -l $SPARK_DIR/conf/slaves | cut -f 1 -d' '`
WORKER_CORES=`grep SPARK_WORKER_CORES $SPARK_DIR/conf/spark-env.sh | cut -f 2 -d'='`
MEMORY=`grep spark.executor.memory $SPARK_DIR/conf/spark-defaults.conf | cut -f 2 | sed -r 's/([0-9]+).*/\1/g'`

echo "changing cluser config ($SLAVES slaves /w $WORKER_CORES cores and $MEMORY MB RAM) ..."

# copy slave definition
cp $SPARK_DIR/conf/slaves $FLINK_DIR/conf/slaves


# remove default value to be changed
FLINK_CONF=$FLINK_DIR/conf/flink-conf.yaml
mv $FLINK_CONF $FLINK_CONF.bkp
grep -v "jobmanager.rpc.address:" $FLINK_CONF.bkp | grep -v "taskmanager.heap.mb" | grep -v "taskmanager.numberOfTaskSlots" | grep -v "parallelism.default" | grep -v "taskmanager.network.numberOfBuffers" > $FLINK_CONF

# appeding new values
echo jobmanager.rpc.address: $MASTER >> $FLINK_CONF
echo fs.hdfs.hadoopconf: $HADOOP_DIR/conf >> $FLINK_CONF
echo taskmanager.heap.mb: $MEMORY >> $FLINK_CONF
echo taskmanager.memory.fraction: 0.7 >> $FLINK_CONF


# some parameters that depend on the number of worker cores
echo taskmanager.numberOfTaskSlots: $WORKER_CORES >> $FLINK_CONF
# cores^2 * #machines * 4RES \* $SLAVES \* 4)
echo taskmanager.network.numberOfBuffers: $(expr $WORKER_CORES \* $WORKER_CORES \* $SLAVES \* 4 \* 8) >> $FLINK_CONF
echo parallelism.default: $(expr $WORKER_CORES \* $SLAVES) >> $FLINK_CONF
#TODO: 
#taskmanager.network.bufferSizeInBytes ?
#jobmanager.heap.mb stays default ?


echo distributing flink config ...
## create a zip of the flink dir and distribut it to the slaves (cleanup before)
#tmp=$(pwd)
#cd $FLINK_DIR
#FLINK_TAR=~/flink.tgz
#tar zcf $FLINK_TAR .
#$SPARK_DIR/sbin/slaves.sh rm -fr $FLINK_DIR
#$SPARK_DIR/sbin/slaves.sh rm -fr $FLINK_TAR
#while read s; do scp $FLINK_TAR root@$s:$FLINK_TAR; done < $FLINK_DIR/conf/slaves
#$SPARK_DIR/sbin/slaves.sh mkdir $FLINK_DIR
#$SPARK_DIR/sbin/slaves.sh tar xzf $FLINK_TAR -C $FLINK_DIR
#cd $tmp
$SPARK_DIR/sbin/slaves.sh rm -fr $FLINK_DIR
$SPARK_DIR-ec2/copy-dir $FLINK_DIR

echo "done!"

echo "to start the flink cluster run $FLINK_DIR/bin/start-cluster.sh"
echo "you might shudown the spark instance in beforhand ($SPARK_DIR/sbin/stop-all.sh)"



