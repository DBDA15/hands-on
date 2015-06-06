#! /bin/bash

echo Tagging custer ...
REGION=`wget -q -O - http://169.254.169.254/latest/dynamic/instance-identity/document|grep region|awk -F\" '{print $4}'`
INSTANCE=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`
GROUP=`ec2-describe-instances -O $AWS_ACCESS_KEY_ID -W $AWS_SECRET_ACCESS_KEY $INSTANCE --region $REGION | grep -P "^INSTANCE" | cut -f 7`

echo -n $INSTANCE > instance_ids
$HADOOP_DIR/bin/slaves.sh  wget -q -O - http://169.254.169.254/latest/meta-data/instance-id  | cut -f 2 -d' ' | awk '{print " "$1}' >> instance_ids
INSTANCES=`cat instance_ids`

ec2-create-tags -O $AWS_ACCESS_KEY_ID -W $AWS_SECRET_ACCESS_KEY --region $REGION $INSTANCES --tag team=$GROUP
