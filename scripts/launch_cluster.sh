#!/bin/bash

# Takes four arguments:
#   bucket name - one that has already been created
#   name of key file - without .pem extension
#   type of instance - type of instance to launch for master and core nodes
#      e.g. m2.4xlarge would launch instances with 8 cores and 64 GB of RAM
#   number of slave instances
#      ex. bash launch_cluster.sh myBucket myPem instanceType 2

# Requires the awscli to be set up, need to have correct default region configured
# Run `aws configure` to set this up

aws s3 cp bootstrap-emr.sh s3://$1/scripts/bootstrap-emr.sh

aws emr create-cluster \
    --name PySparkCluster \
    --release-label emr-5.4.0 \
    --log-uri s3://spark-talk/logs/ \
    --enable-debugging \
    --applications Name=Spark \
    --ec2-attributes KeyName=$2 \
    --use-default-roles \
    --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=$3 \
      InstanceGroupType=CORE,InstanceCount=$4,InstanceType=$3 \
    --bootstrap-actions Path=s3://$1/scripts/bootstrap-emr.sh \
    --configurations file://./myConfig.json
