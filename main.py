#!/usr/bin/python
#
# main.py -
#     combine two tables
#     obtaining the last five transactions performed by
#	      a particular transactionID from a JSON file
#     document summeary or aggreation
#
# Cloud Cho, March 23, 2021
#   For Technical Assesment for Sr. Data Engineer position at Vanguard
#
# Please, export SPARK_LOCAL_HOSTNAME=localhost in bashrc first, thanks.
#
# Input file is located in ~/data/banking/
#
# To to
#    merge dev branch to master - to share code in job application
#      add all function unit test in util_test.py
#      merge to master branch
#      clean code in master branch like error and debug comment
#
#    flatten
#    combine similar column labels -
#      label side: word vector, transformer
#      content side (instead of label, content could give better characteristics)
#       exact matching?, format?, word vector, transformer
#         transformer (large dataset for training)
#
#    file reading
#      cut the comment and header of the example text file
#        PySpark text file reading still requred key and value pair
#    ptint out label with most common content - text_file_modify in util file
#    total number of col and row
#
#    all the "pass" call need to be implemented
#
# Error
#   please check
#
# How to run
#   example
#     Text file reading
#       python main.py --in_file "file:////home/cloud/Desktop/fintech list from growjo 10000.txt" --choice size
#     CSV
#       python main.py --in_file "file:////home/cloud/Desktop/fintech list from growjo 10000.csv"
#
# Runtime enviroment:
#    Vanguard using Anaconda -
#      if you want to use Pathlib
#      need to upgrade to Ubuntu 18.04
#
# Reference:
#    transactionID: https://stackoverflow.com/questions/56518655/obtaining-the-last-five-transactions-performed-by-a-particular-transactionid-fro


import argparse, glob, os, pdb, sys

import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.types import StructField, StringType, DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import length
from pyspark.sql.functions import lit

from datetime import datetime, date
import pandas as pd

from util import PrepDesk, AnalysisDesk
import util

READ_FILE = False
DEBUG = True



# 1st tiral
#~ spark = SparkSession.builder.getOrCreate()

#~ df = spark.createDataFrame([
    #~ Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    #~ Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    #~ Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
#~ ])

#~ print (df)


# 2nd trial
#~ def init_spark():
  #~ spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  #~ sc = spark.sparkContext
  #~ return spark,sc

#~ def main():
  #~ spark,sc = init_spark()
  #~ nums = sc.parallelize([1,2,3,4])
  #~ print(nums.map(lambda x: x*x).collect())

# 3rd tiral
def main():
    # If initialize in the Class call, it would induce multiple SparkContexts
    #   error
    # init_spark()

    args = getting_arg()

    if (READ_FILE):
        # sc = pyspark.SparkContext('local[*]')
        # Cannot run multiple SparkContexts at once
        #   so if you run this line in other file, it induced an error.
        sc = pyspark.SparkContext('local[*]')
        spark = SparkSession.builder.getOrCreate()


        # Read a text file from HDFS, a local file system (available on all nodes),
        # or any Hadoop-supported file system URI, and
        # return it as an RDD of Strings. The text files must be encoded as UTF-8.
        in_txt = sc.textFile('file:////usr/share/doc/python/copyright')
        print("Total line: {}".format(in_txt.count()))

        #
        # To do
        #   what this function do?
        python_lines = in_txt.filter(lambda line: 'python' in line.lower())
        print( "First \"python\" is at line {}?".format( python_lines.count() ) )

    if (args.choice == "read file"):
        if (DEBUG):
            pdb.set_trace()

        work_desk = util.PrepDesk(app_name="Look")
        data_large = work_desk.load_file(args.in_file)

    elif (args.choice == "test"):
        desk = PrepDesk("place_holder")
        print(type(desk))
        dataset = desk.create_table(option='empty')
        print(type(dataset))
        print(dataset.collect())

    elif (args.choice == "merge"):
        mergedDF = combine(args)

    # print out label with most commone contents under the label
    elif (args.choice == "most common"):
        work_desk = util.PrepDesk(app_name="Look")
        data_large = work_desk.load_file(args.in_file)
        work_desk.content_guess(rdd_external = data_large)

    # table total row and column
    elif (args.choice == "size"):
        work_desk = util.PrepDesk(app_name="Look")
        data_large = work_desk.load_file(args.in_file)
        result = work_desk.table_size(rdd_external = data_large)

    else:
       work_desk = util.PrepDesk(app_name="Look")
       data_large = work_desk.load_file(args.in_file)

       if (DEBUG):
           info_size = 10
           print ("First {} data".format(info_size))
           print (data_large.take(info_size))

       anal_desk = util.AnalysisDesk(dataset=data_large)
       #
       # To do
       #   given_period need to implement using "struct"
       #   please test at 00-essay-3.py
       anal_desk.growth(period=given_period)

    pdb.set_trace()


def init_spark():
    #
    # Error
    #   Exception: Java gateway process exited before sending its port number
    #
    # paranthesis just multi line with point possible
    # self.conf = (SparkConf().setMaster('local').setAppName(app_name).
    #     set("spark.executor.memory", "lg"))
    conf = SparkConf().setMaster('local[*]')
    # Cannot run multiple SparkContexts at once
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()


def combine(args):
    # go through JSON files in the folder

    # pdb.set_trace()
    file_type = "*.json"
    if (args.in_folder == None):
        print ("{} folder not exist".format(args.in_folder))
        return 0

    tables = glob.glob(os.path.join(args.in_folder, file_type))
    if len(tables == 0):
        print ("{} not in the {}".format(file_type, args.in_folder))
        return 0

    for idx, each_table in enumerate(tables):

        # Method 1
        # Not common JSON, it should be JSONL type file
        #   https://stackoverflow.com/questions/38895057/reading-json-with-apache-spark-corrupt-record
        # tempDF = spark.read.json(each_table)

        # Method 2
        # Work but field need to be flatten
        tempDF = spark.read.option("multiline", "true").json(each_table)
        fields = flatten(tempDF.schema, None)
        # ['_comment', 'data.peterjak.accounts.5c7072a835c3b.balance', 'data.peterjak.accounts.5c7072a835c3b.name', 'data.peterjak.accounts.5c7696db0745b.balance', 'data.peterjak.accounts.5c7696db0745b.name', 'data.peterjak.name', 'data.peterjak.nikajak.accounts.5c7000098525e.balance', 'data.peterjak.nikajak.name']
        extracted = tempDF.select(fields)


        # Method 3
        # Error
        # TypeError: path can be only string, list or RDD
        # https://stackoverflow.com/a/46745778/5595995
        # tempDF = spark.read.json(spark.sparkContext.wholeTextFiles(each_table).values)

        pdb.set_trace()

        if  (idx == 0):
            mergedDF = tempDF
        else:
            # Check the same field name
            common_field_name = \
                set(mergedDF.schema.fieldNames()) & set(tempDF.schema.fieldNames())
            print ("No of same field name(s): {}".format(
                len(common_field_name)))

            # To do
            #   Find similar field name
            #     First check column label and at most 14% of contents
            #     (1) using longest common substring and longest common sub squence
            #     (2) (exact) word search
            #     (3) word vactor
            #     (4) Large Language Model like Transformer, but this is word
            #       also need to train with large dataset?

            # Combine two tables
            mergedDFtemp = mergedDF.union(tempDF)
            mergedDF = mergedDFtemp

    return mergedDF


# Flatten the column
def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

# extracted = get_change_one.select( flatten(get_change_one.schema) )


def getting_arg():
    parser = argparse.ArgumentParser(description='Make data')
    parser.add_argument('--in_file', dest='in_file',
        help='input file name')
    parser.add_argument('--in_folder', dest='in_folder',
        help='input folder name')
    parser.add_argument('--out_file', dest='out_file',
        help='output file name')
    parser.add_argument('--choice', dest='choice', type=str,
        help='task choice')

    args = parser.parse_args()

    return args


def doc_sum(in_file):
    # "fintech list from growjo 10000.txt"
    #
    # To do
    #   count total words in each line
    #   guess label on the first line
    #   figure out missing column in each line
    return 0


if __name__ == '__main__':
    main()
