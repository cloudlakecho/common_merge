
# util.py - Task for database
# from October 14, 2023
#
# Funtion:
#     Load CSV, SQL file
#     Create`new table
#     Flatten, join, add column, secondary key, new index
#     Aggreate, reduce


import os, pdb, sys
from pyspark import SpsrkContext, SparkConf


# Set PySpark enviornment, create table and modify it
class PrepDesk:
    def __init__(self, app_name=None):
        self.conf = (SparkConf().setMaster('local').setAppName(app_name)
        .set("spark.executor.memory", "lg"))
        self.sc = SparkContext(conf=conf)
    # Load CSV, SQL file
    def load_file(self, input_file) -> :


        # Resilient Dirstirbute Database
        self.rdd = sc.fromfile(input_file)

        return self.rdd


    # Read exist table and create new table
    def create_table(self, exist_table, option='empty') -> :
        no_row = exist_table.count()
        # Attribute (number of columns)
        no_col = exist_table.count_col()


        # Using PySpark
        sc.parallel(size=(no_row, no_col))
        # Using SQL
        sc.sql


    # Flatten, join, add column, secondary key, new index
    def modify_table(self, tables, no_table, task='flatten') -> :
        no_table = len(tables)
        if (no_table == '2'):



# Aggreate
class AnalysisDesk:
    def __init__(self, dataset=None):
        # RDD: Resilient Distributed Dataset
        self.rdd = dataset

    # Aggreate, reduce

    # customer with certian transaction during period
    def find_customer(self, period, amount):


    # Customer account grwoth during month, year or specific period
    def growth(self, date_unit, period):


    # Find top saving or transaction in period
    def top_amount(self, period):
        
