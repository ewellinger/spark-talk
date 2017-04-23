from __future__ import print_function
import os
import sys



import pyspark as ps    # for the pyspark suite
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
import string
import unicodedata

try:
    from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
except:
    print("Current Python Path: {}".format(os.environ['PYTHONPATH']), file=sys.stderr)
    exit(-1)
# import spacy

from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF

from pyspark.ml.clustering import LDA



print("Hello World")

# def extract_bow_from_raw_text(text_as_string):
#     """ Extracts bag-of-words from a raw text string.
#
#     Parameters
#     ----------
#     text (str): a text document given as a string
#
#     Returns
#     -------
#     list : the list of the tokens extracted and filtered from the text
#     """
#     if (text_as_string == None):
#         return []
#
#     if (len(text_as_string) < 1):
#         return []
#
#     # Load nlp object if it isn't accessible
#     if 'nlp' not in globals():
#         global nlp
#         try:
#             # When running locally
#             nlp = spacy.load('en')
#         except RuntimeError:
#             # When running on AWS EMR Cluster
#             nlp = spacy.load('en', via='/mnt/spacy_en_data/')
#
#     # Run through spacy English module
#     doc = nlp(text_as_string)
#
#     # Part's of speech to keep in the result
#     pos_lst = ['ADJ', 'ADV', 'NOUN', 'PROPN', 'VERB']
#
#     # Lemmatize text and split into tokens
#     tokens = [token.lemma_.lower() for token in doc if token.pos_ in pos_lst]
#
#     stop_words = {'book', 'author', 'read', "'", 'character'}.union(ENGLISH_STOP_WORDS)
#
#     # Remove stop words
#     no_stop_tokens = [token for token in tokens if token not in stop_words]
#
#     return(no_stop_tokens)
#
#
# def indexing_pipeline(input_df, **kwargs):
#     """ Runs a full text indexing pipeline on a collection of texts contained
#     in a DataFrame.
#
#     Parameters
#     ----------
#     input_df (DataFrame): a DataFrame that contains a field called 'text'
#
#     Returns
#     -------
#     df : the same DataFrame with a column called 'features' for each document
#     wordlist : the list of words in the vocabulary with their corresponding IDF
#     """
#     inputCol_ = kwargs.get("inputCol", "text")
#     vocabSize_ = kwargs.get("vocabSize", 5000)
#     minDF_ = kwargs.get("minDF", 2.0)
#
#     tokenizer_udf = udf(extract_bow_from_raw_text, ArrayType(StringType()))
#     df_tokens = input_df.withColumn("bow", tokenizer_udf(col(inputCol_)))
#
#     cv = CountVectorizer(inputCol="bow", outputCol="vector_tf", vocabSize=vocabSize_, minDF=minDF_)
#     cv_model = cv.fit(df_tokens)
#     df_features_tf = cv_model.transform(df_tokens)
#
#     idf = IDF(inputCol="vector_tf", outputCol="features")
#     idfModel = idf.fit(df_features_tf)
#     df_features = idfModel.transform(df_features_tf)
#
#     return(df_features, cv_model.vocabulary)


if __name__=='__main__':
    # Get or Create a new SparkSession object
    spark = ps.sql.SparkSession.builder \
                .appName("Spark Talk") \
                .getOrCreate()

    # Extract Spark Context from SparkSession object
    sc = spark.sparkContext

    url = "s3n://galvanize-ds/reviews_Books_5.json.gz"

    # Use SparkSession to read in json object into Spark DataFrame
    reviews = spark.read.json(url)

    reviews.printSchema()

    # Let's subset our DataFrame to keep 10% of the reviews
    review_subset = reviews.select('reviewText', 'overall') \
                           .sample(False, 0.1, 42)

    # Save this subset file to S3
    review_subset.write.save('s3n://spark-talk/reviews_Books_subset2.json',
                             format='json')

    print(review_subset.count())

    review_subset.show(10, truncate=True)



    # review_df, vocab = indexing_pipeline(review_subset, inputCol='reviewText')
    #
    # review_df.printSchema()
    #
    # # Persist this DataFrame to keep it in memory
    # review_df.persist()
    #
    # print("Example of first 20 words in our Vocab:")
    # print(vocab[:20])
    #
    # lda = LDA(k=20, seed=42, featuresCol='features')
    # model = lda.fit(review_df)
    #
    # model_description = model.describeTopics(20)
    #
    # # Let's save the model description
    # model_description.write.save('s3n://spark-talk/lda_model_description.json',
    #                              format='json')

    sc.stop()
