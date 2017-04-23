#!/bin/bash

# Takes four arguments:
#   bucket name - one that has already been created
#	cluster-id - Id of an already running cluster
#	name to give step
#   path to python file to execute

aws s3 cp $4 s3://$1/scripts/$4

aws emr add-steps --cluster-id $2 --steps Type=spark,Name=$3,Args=[--deploy-mode,cluster,--master,yarn,s3://$1/scripts/$4],ActionOnFailure=CONTINUE



# export HADOOP_CONF_DIR=/etc/hadoop/conf
# spark-submit \
# 	--class spark-talk.py \
# 	--master yarn \
# 	--deploy-mode cluster \
# 	--packages com.databricks:spark-csv_2.11:1.5.0 \
# 	--packages com.amazonaws:aws-java-sdk-pom:1.10.34 \
# 	--packages org.apache.hadoop:hadoop-aws:2.7.3
