from __future__ import print_function
from builtins import *
import numpy as np
import pandas as pd
import pyspark as ps
from spark_talk import S3Logging, load_numpy_from_s3


# class S3Logging(object):
#     """
#     Object allowing for printing logs to an S3 file
#
#     Useful for iteratively logging messsages in a long running script such as a
#     Spark application where stdout is only available upon completion.
#
#     NOTE: S3 does not allow appending to an already existing file and so your
#     specified log will be rewritten upon each call to `push_log()`
#
#     NOTE: Must have previously configured the awscli tools
#     """
#     def __init__(self, bucket, fname, tstamp=True, redirect_stderr=False, redirect_stdout=False, push=False, overwrite_existing=False):
#         """
#         Args:
#             bucket (str): S3 Bucket name
#             fname (str): Name to give to log file
#             tstamp (bool): default True
#                 Whether to include a timestamp with each call to write
#             redirect_stderr (bool): default False
#                 Direct all stderr messages to be logged
#             redirect_stdout (bool): default False
#                 Direct all stdout messages to be logged
#                 NOTE: run `sys.stdout = sys.__stdout__` to restore default
#                 behavior
#             push (bool): default False
#                 Copy log to S3 upon each call to write()
#             overwrite_existing (bool): default False
#                 Whether to overwrite file if it already exists.  If False and
#                 the file does already exist, messages will be appended to the
#                 file
#         """
#         self._s3 = boto3.client('s3')
#         self.bucket = bucket
#         self.key = fname
#         self._tstamp = tstamp
#         self._push = push
#
#         if redirect_stderr:
#             # redirect all stderr outputs to write to self
#             sys.stderr = self
#
#         if redirect_stdout:
#             # redirect all stdout outputs to write to self
#             sys.stdout = self
#
#         if not overwrite_existing and self._exists():
#             body_obj = self._s3.get_object(Bucket=self.bucket, Key=self.key)['Body']
#             # self._msg = str(body_obj.read(), 'utf-8')
#             if sys.version_info.major < 3:
#                 self._msg = str(body_obj.read())
#             else:
#                 self._msg = str(body_obj.read(), 'utf-8')
#         else:
#             self._msg = ''
#
#     def write(self, msg, push=None):
#         if push is None:
#             push = self._push
#
#         # Append message with or without timestamp
#         if self._tstamp and bool(msg):
#             self._msg += "\n{0}\n{1}\n".format(datetime.now(), msg)
#         else:
#             self._msg += "\n{0}\n".format(msg)
#
#         if push:
#             self.push_log()
#
#     def push_log(self):
#         if sys.version_info.major < 3:
#             f_handle = StringIO(unicode(self._msg))
#         else:
#             f_handle = StringIO(self._msg)
#         self._s3.put_object(Bucket=self.bucket, Key=self.key, Body=f_handle.read())
#
#     def restore_stdout(self):
#         sys.stdout = sys.__stdout__
#
#
#     def _exists(self):
#         bucket = boto3.resource('s3').Bucket(self.bucket)
#         objs = list(bucket.objects.filter(Prefix=self.key))
#         return len(objs) > 0 and objs[0].key == self.key
#
#     def __repr__(self):
#         return self._msg
#
#
# def load_numpy_from_s3(bucket, fname):
#     s3 = boto3.client('s3')
#     body_obj = s3.get_object(Bucket=bucket, Key=fname)['Body']
#     temp = BytesIO(body_obj.read())
#     return np.load(temp)


if __name__=='__main__':
    # Get or Create a new SparkSession object
    spark = ps.sql.SparkSession.builder \
                .master('local[4]') \
                .appName("Spark Talk") \
                .getOrCreate()

    model_descrip_path = 's3n://spark-talk/lda_model_description'

    log = S3Logging('spark-talk', 'application-log2.txt', redirect_stdout=True)

    model_description = spark.read.json(model_descrip_path)
    vocab = load_numpy_from_s3('spark-talk', 'vocab_array.npz')['vocab']

    model_description_df = model_description.toPandas()

    for idx, row in model_description_df.iterrows():
        desc = "Top Words Associated with Topic {0}:\n{1}\n" \
                    .format(row['topic'], vocab[row['termIndices']])
        print(desc)
    log.push_log()
    log.restore_stdout()
