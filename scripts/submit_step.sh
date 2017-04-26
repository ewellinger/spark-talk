#!/bin/bash

# Takes four arguments:
#   bucket name - one that has already been created
#	cluster-id - Id of an already running cluster
#   path to python file to execute
#	name to give step

aws s3 cp $3 s3://$1/scripts/$3

aws emr add-steps --cluster-id $2 --steps Type=spark,Name=$4,Args=[--deploy-mode,cluster,--master,yarn,s3://$1/scripts/$3],ActionOnFailure=CONTINUE
