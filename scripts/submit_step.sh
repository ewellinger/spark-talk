#!/bin/bash

# Takes four arguments:
#   bucket name - one that has already been created
#	cluster-id - Id of an already running cluster
#	name to give step
#   path to python file to execute

aws s3 cp $4 s3://$1/scripts/$4

aws emr add-steps --cluster-id $2 --steps Type=spark,Name=$3,Args=[--deploy-mode,cluster,--master,yarn,s3://$1/scripts/$4],ActionOnFailure=CONTINUE
