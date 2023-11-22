
# util.py - Task for dataset (DSS so only PySpark)
# from October 14, 2023
#
# Funtion:
#     Load CSV, SQL file
#     Create`new table
#     Flatten, join, add column, secondary key, new index
#     Aggreate, reduce


import os, pdb, sys
import Pathlib  # Python 3.8 and later
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Set PySpark enviornment, create table and modify it
class PrepDesk:
    def __init__(self, app_name=None):
        self.conf = (SparkConf().setMaster('local').setAppName(app_name)
            .set("spark.executor.memory", "lg"))
        self.sc = SparkContext(conf=self.conf)
        self.spark = SparkSession.builder.getOrCreate()

    # Load CSV, SQL file
    def load_file(self, input_file):
        if (input_file == None):
            self.rdd = self.sc.parallelize(range(100))
        else:
            if (file_type == "txt"):
                # Resilient Dirstirbute Database
                self.rdd = self.sc.textfile(input_file)
            elif (file_type == "json");
               self.rdd = self.spark.read.jason(input_file) \
                   .createOrReplaceTempView("customer")
            elif (file_type == "csv"):
                pass

            else:
                print ("Not readable")
                sys.exit(1)


        return self.rdd


    # Read exist table and create new table
    def create_table(self, exist_table, option='empty', para=None):
        if (type(exist_table) == "type<spark>"):
            no_row = exist_table.count()
            # Attribute (number of columns)
            no_col = exist_table.count_col()

        if (option == 'empty'):
            #
            # To do
            #     Find this method
            self.rdd = self.sc.sql("CREATE TABLE ({} {} {})".format(
                para.title, para.no_row, para.no_col))
        else:
            # Using SQL
            self.rdd = self.sc.sql("CREATE TABLE {} ( \
              {} {} PRIMARY KEY \
              {} {} FOREIGN KEY \
              REFERENCE {} {})".format(
                para.title, para.col_name[0], para.col_type[0],
                para.col_name[1], para.col_type[1],
                para.other_title, para.other_col))

        return self.rdd


    # Flatten, join, add column, secondary key, new index
    def modify_table(self, tables, task='flatten', para=None):
        no_table = len(tables)

        if (task == 'flatten'):
            pass

        if (task == 'join'):

            if (no_table != 2):
                print ("Please, check total number of tables.")
                return None;

            self.spark.sql("SELECT {} \
                FROM {} \
                INNER JOIN \
                ON {} = {}".format(para.col_name,
                    para.table_name, para.comp_col[0],
                    para.com_col[1]
                ))

        if (task == 'add column'):
            self.spark.sql("ALTAR TABLE {} \
                INSERT COLUMN {} {} \
                DEFAULT {}".format(para.title, para.col_name,
                para.col_type, para.val))
            self.sc

        if (task == "secondary key"):
            self.spark.sql("ALTAR TABLE {} \
            INSERT COLUMN {} {} \
            DEFAULT {} \
            SECONDAR KEY".format (para.title, para.col_name,
            para.col_type))
        #
        # To do
        #
        if (task == "new index"):
            pass


# Aggreate
class AnalysisDesk:
    #
    # To do
    #   Make child class of PrepDesk
    #
    def __init__(self, dataset=None):
        if (dataset == None):
            self.rdd = sc.parallelize(range(100))
        else:
            # RDD: Resilient Distributed Dataset
            self.rdd = dataset


    # Aggreate, reduce

    # customer with certian transaction during period
    def find_customer(self, period, amount, comp):
        # each customer sum during period
        rdd_part = self.rdd
        rdd_part.filter(lambda x, y: y >= period.start)
        rdd_part.filter(lambda x, y: y <= period.end)

        rdd_part.reduceByKey(lambda x, y: x + y)
        # if (comp == 'exact'):
        #     rdd_part.filter(lambda x, y: y == amount)
        # if (comp == 'more'):
        #     rdd_part.filter(lambda x, y: y => amount)
        # if (comp == 'less'):
        #     rdd_part.filter(lambda x, y: y =< amount)

    # Customer account grwoth during month, year or specific period
    def growth(self, date_unit, period):
       # each customer sum in each month or each year
       pass


    # Find top saving or transaction in period
    def top_amount(self, period):
       # sc.sortByKey
       # find largest
       pass
