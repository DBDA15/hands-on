#! /bin/bash

echo upgrading AWS CLI ...
easy_install --upgrade awscli

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

rm -f resource_ids
cp instance_ids resource_ids

# tag attached ebs volumes
INSTANCE_FILTER=`echo '"'$INSTANCES'"' | awk '{$1=$1}1' OFS="\",\"" | awk '{printf "[{\"Name\":\"attachment.instance-id\",\"Values\":[%s]}]",$1}'`
aws ec2 describe-volumes --region $REGION --filters $INSTANCE_FILTER | grep "VolumeId" | cut -d':' -f 2 | cut -d'"' -f 2 | awk '{printf " %s",$0} END {print ""}' >> resource_ids

# tag all instances and volumes
RESOURCES=`cat resource_ids`
ec2-create-tags -O $AWS_ACCESS_KEY_ID -W $AWS_SECRET_ACCESS_KEY --region $REGION $RESOURCES --tag team=$GROUP
