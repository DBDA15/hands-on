#! /bin/bash

echo Tagging custer ...
# get master instance id
rm -f instance_id
wget -q -O instance_id http://169.254.169.254/latest/meta-data/instance-id
INSTANCE=`cat instance_id`

# get group and region data
REGION=`wget -q -O - http://169.254.169.254/latest/dynamic/instance-identity/document|grep region|awk -F\" '{print $4}'`
GROUP=`ec2-describe-instances -O $AWS_ACCESS_KEY_ID -W $AWS_SECRET_ACCESS_KEY $INSTANCE --region $REGION | grep -P "^INSTANCE" | cut -f 7`

# collect all instance ids (incl. slaves)
echo -n $INSTANCE > instance_ids
$HADOOP_DIR/bin/slaves.sh rm -f instance_id
$HADOOP_DIR/bin/slaves.sh wget -q -O instance_id http://169.254.169.254/latest/meta-data/instance-id
$HADOOP_DIR/bin/slaves.sh awk 1 instance_id |  cut -f 2 -d' ' | awk '{printf " %s",$0} END {print ""}' >> instance_ids
INSTANCES=`cat instance_ids`

# tag all instances
ec2-create-tags -O $AWS_ACCESS_KEY_ID -W $AWS_SECRET_ACCESS_KEY --region $REGION $INSTANCES --tag team=$GROUP
