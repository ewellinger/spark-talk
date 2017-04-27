# Apache Spark

The slides are located in spark-talk.pdf.

**NOTE**: Some of the included scripts require previous configuration of the AWS CLI tools (i.e. you must have run `aws configure` to set this up...)


## Scripts

### Launch Cluster

The `launch_cluster.sh` script allows you to launch a Amazon EMR cluster from the command line.  This will copy the `bootstrap-emr.sh` script to a specified s3 bucket (this must already be created) and launch a spark cluster with the specified number of core instances and instance type.

For example:

```bash
bash launch_cluster.sh spark-talk myPem m2.4xlarge 4
```

... would launch a cluster pointing to a bucket called `spark-talk` with 1 master and 4 slave m2.4xlarge instances.  Please refer to the comments in the `launch_cluster.sh` file for more information


### Submit Step

The `submit_step.sh` script will allow you to submit a script as a step to an AWS EMR cluster.  It takes in four arguments; bucket name (where to upload the script to), cluster-id (the id of an already running cluster), path to python file, and the name to give the step.

For example:

```bash
bash submit_step.sh spark-talk j-XXXXXXXXXXXXX spark_talk.py LDAExample
```


### Jupyter Notebook EMR

The `jupyspark-emr.sh` script allows you to launch a Jupyter notebook on the master of a running EMR cluster.  This requires that you `ssh` into the master of the cluster and run the script from there.
