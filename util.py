#
# util.py - Task for dataset (DSS so only PySpark)
# from October 14, 2023
#
# Funtion:
#     Load CSV, SQL file
#     Create`new table
#     Flatten, join, add column, secondary key, new index
#     Aggreate, reduce
#
# Assumption
#   ID, amount, period start, period end
# To do
#     Please, check in the code
#


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

    # Load JSON, CSV, SQL file
    def load_file(self, input_file):
        if (input_file == None):
            self.rdd = self.sc.parallelize(range(100))
        else:
            if (file_type == "txt"):
                # Resilient Dirstirbute Database
                self.rdd = self.sc.textfile(input_file)
            elif (file_type == "json"):
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
            # spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING,
            # delay INT, distance INT, origin STRING, destination STRING)")
            self.rdd = self.sc.sql("CREATE TABLE {} ({} {}, {} {}, {} {})".format(
                para.title, para.col_name[0], para.col_type[0],
                para.col_name[1], para.col_type[1],
                para.col_name[2], para.col_type[2]))
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


    def flatten(self, schema, prefix=None):
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


    # Flatten, join, add column, secondary key, new index
    def modify_table(self, tables, task='flatten', para=None):
        no_table = len(tables)

        if (task == 'flatten'):
            fields = flatten(self.schema, self.prefix)

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


        if (task == "secondary key"):
            self.spark.sql("ALTAR TABLE {} \
            INSERT COLUMN {} {} \
            DEFAULT {} \
            SECONDAR KEY".format (para.title, para.col_name,
            para.col_type))
        #
        # To do
        #   selection on column as index
        if (task == "new index"):
            if (not (para.new_index in extracted.select('columnnames').collect())):
                print ("{} column not existed, so setting index failed".format(
                  para.newe_index))
                ))
            else:
                            pass




# Aggreate
class AnalysisDesk(PrepDesk):
    def __init__(self, dataset=None):
        super().__init__(dataset)
        if (dataset == None):
            self.rdd = sc.parallelize(range(100))
        else:
            # RDD: Resilient Distributed Dataset
            self.rdd = dataset

    # Aggreate, reduce

    # Customer with certian transaction during period
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
        # Each customer during period
        rdd_part = self.rdd
        #   if date_unit is year, start becomes january 1 and
        #   end December 31
        rdd_part.filter(lambda x, y: y >= period.start)
        rdd_part.filter(lambda x, y: y <= period.end)
        # Find earliest and latest account info in each customer

        # Latest minus earlist

        pass


    # Find top saving or transaction in period
    def top_amount(self, period):
       rdd_part.filter(lambda x, y: y >= period.start)
       rdd_part.filter(lambda x, y: y <= period.end)

       # sc.sortByKey and find largest
       #  page 215 at Holden's Learning PySpark
       #  https://stackoverflow.com/a/59134436/5595995
       rdd_part.groupBy("ID").avg("amount")\
         .orderBy(desc("avg(amount)")).show(1)
