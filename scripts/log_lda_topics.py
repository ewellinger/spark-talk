from __future__ import print_function
from builtins import *
import numpy as np
import pandas as pd
import pyspark as ps
import boto3
import botocore
from spark_talk import S3Logging, load_numpy_from_s3


if __name__=='__main__':
    # Get or Create a new SparkSession object
    spark = ps.sql.SparkSession.builder \
                .master('local[4]') \
                .appName("Spark Talk") \
                .getOrCreate()

    # Load in vocab array
    vocab = load_numpy_from_s3('spark-talk', 'vocab_array.npz')['vocab']

    # Open reference to append to created log in s3
    log = S3Logging('spark-talk', 'application-log.txt', redirect_stdout=True)

    # Create list of all lda model descriptions in s3
    bucket = boto3.resource('s3').Bucket('spark-talk')
    objs = list(bucket.objects.filter(Prefix='lda_'))
    fnames = {obj.key.split('/')[0] for obj in objs}

    for fname in fnames:
        model_descrip_path = 's3n://spark-talk/{}'.format(fname)
        model_description = spark.read.json(model_descrip_path)
        model_description_df = model_description.toPandas()
        print(fname.replace('.json', ''))
        for idx, row in model_description_df.iterrows():
            desc = "Top Words Associated with Topic {0}:\n{1}\n" \
                        .format(row['topic'], vocab[row['termIndices']])
            print(desc)
    log.push_log()
    log.restore_stdout()
