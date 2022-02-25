
# main.py -
#     combine two tables
#     obtaining the last five transactions performed by
#	      a particular transactionID from a JSON file

# Cloud Cho, March 23, 2021
# For Technical Assesment for Sr. Data Engineer position at Vanguard

# Please, export SPARK_LOCAL_HOSTNAME=localhost in bashrc first, thanks.

# Input file is located in ~/data/banking/

# To to
#    flatten
#    combine similar column labels

# Error
#

# Reference:
#    transactionID: https://stackoverflow.com/questions/56518655/obtaining-the-last-five-transactions-performed-by-a-particular-transactionid-fro
import argparse, glob, os, pdb, sys

import pyspark
import pyspark.sql.functions as f
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


READ_FILE = False


sc = pyspark.SparkContext('local[*]')
spark = SparkSession.builder.getOrCreate()



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
    args = getting_arg()

    if (READ_FILE):
        # sc = pyspark.SparkContext('local[*]')
        # Read a text file from HDFS, a local file system (available on all nodes),
        # or any Hadoop-supported file system URI, and
        # return it as an RDD of Strings. The text files must be encoded as UTF-8.
        txt = sc.textFile('file:////usr/share/doc/python/copyright')
        print(txt.count())

        python_lines = txt.filter(lambda line: 'python' in line.lower())
        print(python_lines.count())

    mergedDF = combine(args)

    pdb.set_trace()


def combine(args):
    # go through JSON files in the folder

    # pdb.set_trace()

    tables = glob.glob(os.path.join(args.in_folder, "*.json"))
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
            #   using longest common substring and longest common sub squence

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
    parser.add_argument('--choice', dest='choice', type=int,
        help='data type choice')

    args = parser.parse_args()

    return args


if __name__ == '__main__':
  main()
