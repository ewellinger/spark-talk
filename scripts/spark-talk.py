from __future__ import print_function
import os
import sys
# Modify path for importing packages
paths = ['/home/hadoop/anaconda/bin',
 '/home/hadoop/anaconda/lib/python27.zip',
 '/home/hadoop/anaconda/lib/python2.7',
 '/home/hadoop/anaconda/lib/python2.7/plat-linux2',
 '/home/hadoop/anaconda/lib/python2.7/lib-tk',
 '/home/hadoop/anaconda/lib/python2.7/lib-old',
 '/home/hadoop/anaconda/lib/python2.7/lib-dynload',
 '/home/hadoop/anaconda/lib/python2.7/site-packages',
 '/home/hadoop/anaconda/lib/python2.7/site-packages/Sphinx-1.4.6-py2.7.egg',
 '/home/hadoop/anaconda/lib/python2.7/site-packages/setuptools-27.2.0-py2.7.egg']
sys.path = paths + sys.path

# Import packages for S3Logging Class
try:
    import boto3
except:
    print(sys.path)
    exit(-1)
import botocore
from io import StringIO
from datetime import datetime

# Import packages for spark application
import pyspark as ps    # for the pyspark suite
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
import string
import unicodedata
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
import spacy

from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF

from pyspark.ml.clustering import LDA


class S3Logging(object):
    """
    Object allowing for printing logs to an S3 file

    Useful for iteratively logging messsages in a long running script such as a
    Spark application where stdout is only available upon completion.

    NOTE: S3 does not allow appending to an already existing file and so your
    specified log will be rewritten upon each call to `push_log()`

    NOTE: Must have previously configured the awscli tools
    """
    def __init__(self, bucket, fname, tstamp=True, redirect_stderr=False, push=False, overwrite_existing=False):
        """
        Args:
            bucket (str): S3 Bucket name
            fname (str): Name to give to log file
            tstamp (bool): default True
                Whether to include a timestamp with each call to write
            redirect_stderr (bool): default False
                Direct all stderr messages to be logged
            push (bool): default False
                Copy log to S3 upon each call to write()
            overwrite_existing (bool): default False
                Whether to overwrite file if it already exists.  If False and
                the file does already exist, messages will be appended to the
                file
        """
        self._s3 = boto3.client('s3')
        self.bucket = bucket
        self.key = fname
        self._tstamp = tstamp
        self._push = push

        if redirect_stderr:
            # redirect all stderr outputs to write to self
            sys.stderr = self

        if not overwrite_existing and self._exists():
            body_obj = self._s3.get_object(Bucket=self.bucket, Key=self.key)['Body']
            self._msg = str(body_obj.read(), 'utf-8')
        else:
            self._msg = ''

    def write(self, msg, push=None):
        if push is None:
            push = self._push

        # Append message with or without timestamp
        if self._tstamp:
            self._msg += "\n{0}\n{1}\n".format(datetime.now(), msg)
        else:
            self._msg += "\n{0}\n".format(msg)

        if push:
            self.push_log()

    def push_log(self):
        if sys.version_info.major < 3:
            f_handle = StringIO(unicode(self._msg))
        else:
            f_handle = StringIO(self._msg)
        self._s3.put_object(Bucket=self.bucket, Key=self.key, Body=f_handle.read())

    def _exists(self):
        bucket = boto3.resource('s3').Bucket(self.bucket)
        objs = list(bucket.objects.filter(Prefix=self.key))
        return len(objs) > 0 and objs[0].key == self.key

    def __repr__(self):
        return self._msg


def extract_bow_from_raw_text(text_as_string):
    """ Extracts bag-of-words from a raw text string.

    Parameters
    ----------
    text (str): a text document given as a string

    Returns
    -------
    list : the list of the tokens extracted and filtered from the text
    """
    if (text_as_string == None):
        return []

    if (len(text_as_string) < 1):
        return []

    # Load nlp object if it isn't accessible
    if 'nlp' not in globals():
        global nlp
        try:
            # When running locally
            nlp = spacy.load('en')
        except RuntimeError:
            # When running on AWS EMR Cluster
            nlp = spacy.load('en', via='/mnt/spacy_en_data/')

    # Run through spacy English module
    doc = nlp(text_as_string)

    # Part's of speech to keep in the result
    pos_lst = ['ADJ', 'ADV', 'NOUN', 'PROPN', 'VERB']

    # Lemmatize text and split into tokens
    tokens = [token.lemma_.lower() for token in doc if token.pos_ in pos_lst]

    stop_words = {'book', 'author', 'read', "'", 'character'}.union(ENGLISH_STOP_WORDS)

    # Remove stop words
    no_stop_tokens = [token for token in tokens if token not in stop_words]

    return(no_stop_tokens)


def indexing_pipeline(input_df, **kwargs):
    """ Runs a full text indexing pipeline on a collection of texts contained
    in a DataFrame.

    Parameters
    ----------
    input_df (DataFrame): a DataFrame that contains a field called 'text'

    Returns
    -------
    df : the same DataFrame with a column called 'features' for each document
    wordlist : the list of words in the vocabulary with their corresponding IDF
    """
    inputCol_ = kwargs.get("inputCol", "text")
    vocabSize_ = kwargs.get("vocabSize", 5000)
    minDF_ = kwargs.get("minDF", 2.0)

    tokenizer_udf = udf(extract_bow_from_raw_text, ArrayType(StringType()))
    df_tokens = input_df.withColumn("bow", tokenizer_udf(col(inputCol_)))

    cv = CountVectorizer(inputCol="bow", outputCol="vector_tf", vocabSize=vocabSize_, minDF=minDF_)
    cv_model = cv.fit(df_tokens)
    df_features_tf = cv_model.transform(df_tokens)

    idf = IDF(inputCol="vector_tf", outputCol="features")
    idfModel = idf.fit(df_features_tf)
    df_features = idfModel.transform(df_features_tf)

    return(df_features, cv_model.vocabulary)


if __name__=='__main__':
    log = S3Logging('spark-talk', 'application-log.txt', overwrite_existing=True)

    log.write("Please work", True)

    # Get or Create a new SparkSession object
    spark = ps.sql.SparkSession.builder \
                .appName("Spark Talk") \
                .getOrCreate()

    # Extract Spark Context from SparkSession object
    sc = spark.sparkContext
    # sc.setLogLevel('ERROR')

    # Create logging object for writing to S3
    # log = S3Logging('spark-talk', 'application-log.txt', overwrite_existing=True, redirect_stderr=True)

    # Use SparkSession to read in json object into Spark DataFrame
    url = "s3n://galvanize-ds/reviews_Books_5.json.gz"
    reviews = spark.read.json(url)

    # log.write(reviews.printSchema(), True)

    # Let's subset our DataFrame to keep 5% of the reviews
    review_subset = reviews.select('reviewText', 'overall') \
                           .sample(False, 0.02, 42)

    # Save this subset file to S3
    # review_subset.write.save('s3n://spark-talk/reviews_Books_subset2.json',
    #                          format='json')

    print(review_subset.count())
    log.write("Successfully counted shit", True)
    # log.write(review_subset.count())
    #
    # log.write(review_subset.show(10, truncate=True), True)



    review_df, vocab = indexing_pipeline(review_subset, inputCol='reviewText')

    review_df.printSchema()

    # Persist this DataFrame to keep it in memory
    review_df.persist()

    print("Example of first 20 words in our Vocab:")
    print(vocab[:20])

    lda = LDA(k=10, seed=42, featuresCol='features')
    model = lda.fit(review_df)

    model_description = model.describeTopics(20)

    # Let's save the model description
    model_description.write.save('s3n://spark-talk/lda_model_description.json',
                                 format='json')

    sc.stop()
