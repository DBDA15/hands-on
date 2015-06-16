#! /bin/bash

# setting necessary variables
HADOOP_DIR=/root/ephemeral-hdfs
read -p "Please enter the path to Hadoop home (default: $HADOOP_DIR): " input;
export HADOOP_DIR=${input:-$HADOOP_DIR}

AWS_ACCESS_KEY_ID=$(cat $HADOOP_DIR/conf/core-site.xml | grep -Pzo "(?si)<name>fs.s3n.awsAccessKeyId</name>.*?<value>\K\N+(?=</value>)")
read -p "Please enter the AWS Access Key ID (default: $AWS_ACCESS_KEY_ID): " input;
export AWS_ACCESS_KEY_ID=${input:-$AWS_ACCESS_KEY_ID}

AWS_SECRET_ACCESS_KEY=$(cat $HADOOP_DIR/conf/core-site.xml | grep -Pzo "(?si)<name>fs.s3n.awsSecretAccessKey</name>.*?<value>\K\N+(?=</value>)")
read -p "Please enter the AWS Secret Access Key (default: $AWS_SECRET_ACCESS_KEY): " input;
export AWS_SECRET_ACCESS_KEY=${input:-$AWS_SECRET_ACCESS_KEY}

SPARK_DIR=$(find "/root/spark" 2> /dev/null | grep "ec2/spark-ec2" | sed s:/ec2/spark-ec2$::g)
SPARK_DIR=${SPARK_DIR:-"/root/spark"}
read -p "Please enter the path to Spark home (default: $SPARK_DIR): " input;
export SPARK_DIR=${input:-$SPARK_DIR}

FLINK_BINARY_URL="http://www.eu.apache.org/dist/flink/flink-0.9.0-milestone-1/flink-0.9.0-milestone-1-bin-hadoop1.tgz"
read -p "Please enter the URL to the Flink binaries (default: $FLINK_BINARY_URL): " input;
FLINK_BINARY_URL=${input:-$FLINK_BINARY_URL}

FLINK_DIR=/root/flink
read -p "Please enter the path to Spark home (default: $FLINK_DIR): " input;
export FLINK_DIR=${input:-$FLINK_DIR}

echo Downloadng Flink binaries...
rm -f flink.tgz
wget -O flink.tgz $FLINK_BINARY_URL

echo extracing ...
rm -rf $FLINK_DIR && mkdir $FLINK_DIR
tmp="$FLINK_DIR/$(tar xzfv flink.tgz -C $FLINK_DIR/ | grep "conf/flink-conf.yaml" | sed s:/conf/flink-conf.yaml$::g)"
mv $tmp/* $FLINK_DIR && rm -rf $tmp 
rm -f flink.tgz

export MASTER=`cat $SPARK_DIR-ec2/masters`
SLAVES=`wc -l $SPARK_DIR/conf/slaves | cut -f 1 -d' '`
WORKER_CORES=`grep SPARK_WORKER_CORES $SPARK_DIR/conf/spark-env.sh | cut -f 2 -d'='`
MEMORY=`grep spark.executor.memory $SPARK_DIR/conf/spark-defaults.conf | cut -f 2 | sed -r 's/([0-9]+).*/\1/g'`

echo "changing cluser config ($SLAVES slaves /w $WORKER_CORES cores and $MEMORY MB RAM) ..."

# link to spark slave definition
rm -f $FLINK_DIR/conf/slaves && ln -s $SPARK_DIR/conf/slaves $FLINK_DIR/conf/slaves

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
# cores^2 * #machines * 4RES \* $SLAVES \* 4 (default / min: 2048)
echo taskmanager.network.numberOfBuffers: $(echo -e "2048\n$(expr $WORKER_CORES \* $WORKER_CORES \* $SLAVES \* 4 \* 8)" | awk '$0>x{x=$0};END{print x}') >> $FLINK_CONF
echo parallelism.default: $(expr $WORKER_CORES \* $SLAVES) >> $FLINK_CONF
#TODO: 
#taskmanager.network.bufferSizeInBytes ?
#jobmanager.heap.mb stays default ?

echo distributing flink ...
$SPARK_DIR/sbin/slaves.sh rm -fr $FLINK_DIR
$SPARK_DIR-ec2/copy-dir $FLINK_DIR

echo "creating startup scripts ..."
tmp=~/bin
mkdir $tmp 2>/dev/null
echo -e "#! /bin/bash\n$HADOOP_DIR/bin/stop-all.sh\n$FLINK_DIR/bin/stop-cluster.sh" > $tmp/stop-flink.sh
echo -e "#! /bin/bash\nif [ -n \"\$MASTER\" ] && [ \"\$MASTER\" != \"\`grep 'jobmanager.rpc.address' $FLINK_CONF | cut -d' ' -f 2\`\" ] ; then\n mv -f $FLINK_CONF $FLINK_CONF.bkp2 && sed '/jobmanager.rpc.address/d' $FLINK_CONF.bkp2 > $FLINK_CONF && echo jobmanager.rpc.address: \$MASTER >> $FLINK_CONF && $SPARK_DIR-ec2/copy-dir $FLINK_DIR/conf ;\nfi\n\n$HADOOP_DIR/bin/start-all.sh\n$FLINK_DIR/bin/start-cluster.sh" > $tmp/start-flink.sh
echo -e "#! /bin/bash\n$tmp/stop-spark.sh\n$tmp/start-flink.sh" > $tmp/init-flink.sh
echo -e "#! /bin/bash\n$HADOOP_DIR/bin/stop-all.sh\n$SPARK_DIR/sbin/stop-all.sh" > $tmp/stop-spark.sh
echo -e "#! /bin/bash\n$HADOOP_DIR/bin/start-all.sh\n$SPARK_DIR/sbin/start-all.sh" > $tmp/start-spark.sh
echo -e "#! /bin/bash\n$tmp/stop-flink.sh\n$tmp/start-spark.sh" > $tmp/init-spark.sh
chmod u+x $tmp/init-flink.sh $tmp/init-spark.sh $tmp/stop-flink.sh $tmp/stop-spark.sh $tmp/start-flink.sh $tmp/start-spark.sh
export PATH=$PATH:$tmp

echo "export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> ~/.bashrc
echo "export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> ~/.bashrc
echo "export HADOOP_DIR=$HADOOP_DIR" >> ~/.bashrc
echo "export SPARK_DIR=$SPARK_DIR" >> ~/.bashrc
echo "export FLINK_DIR=$FLINK_DIR" >> ~/.bashrc
echo "export MASTER=\`cat \$SPARK_DIR-ec2/masters\`" >> ~/.bashrc
echo "export PATH=\$PATH:$tmp" >> ~/.bashrc

# final message
echo
echo "flink setup finished!"
echo
echo "to start the flink cluster run init-flink.sh (for spark init-spark.sh, respectively)"
