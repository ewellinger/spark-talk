


### RDDs

* Spark's resilent distributed datasets (RDDs) are how we perform in-memory computations
* An RDD is a collection of immutable partitions of data that are distributed across the nodes of a cluster.


### Transformations And Actions




```bash
aws emr add-steps --cluster-id j-V0PWSYOCK4ZW --steps Type=spark,Name=BookLDAExample,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,spark-talk.py],ActionOnFailure=CONTINUE
```
