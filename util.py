#
# util.py - Task for dataset (DSS so only PySpark)
# from October 14, 2023
#
# Funtion:
#     Load TXT, CSV, SQL file
#     Create`new table
#     Flatten, join, add column, secondary key, new index
#     Aggreate, reduce
#
# Assumption
#   ID, amount, period start, period end
#
# To do
#     Please, check in the code
#


import os, pdb, sys
if (int(sys.version_info.major) > 3) or \
    ((int(sys.version_info.major) == 3) and \
    (int(sys.version_info.minor) >= 8)):
    import Pathlib  # Python 3.8 and later
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

DEBUG = True

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


# Set PySpark enviornment, create table and modify it
class PrepDesk:
    def __init__(self, app_name=None):
        self.sc, self.spark = sc, spark

    # Load JSON, CSV, SQL(DB), TXT file
    def load_file(self, input_file):
        self.text_info = dict()
        if (input_file == None):
            no_partitian = 10
            # need to be key and value pair so "zip" used
            #   reference: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.parallelize.html
            self.rdd = self.sc.parallelize(
               zip(range(100), range(100)), numSlices=no_partitian)

            if (DEBUG):
                pdb.set_trace()

        else:
            # Could be error, Pathlib would be robust
            file_type = input_file.split('.')[1]
            if (file_type == "txt"):
                self.txt_info = self.text_file_modify(input_file)
                #
                # To do
                #   truncate until header
                #
                if (DEBUG):
                    pdb.set_trace()

                # Resilient Dirstirbute Database
                # RDDs are schema-less data structures
                #
                # To do
                #   check the RDD looks like
                #
                self.rdd_spark = self.sc.textFile(input_file)


            elif (file_type == "json"):
               self.rdd = self.spark.read.jason(input_file) \
                   .createOrReplaceTempView("customer")
            elif (file_type == "csv"):
                pass

            else:
                # To do
                #   some text file don't have file type in file name
                #
                print ("Not readable")

                sys.exit(1)


        return self.rdd

    # Remove comment, header and
    def text_file_modify(self, input_file):
        col_count_cur = 0
        col_count_pre = 0
        header, first_row = str(), str()
        line_idx = 0
        #
        # To do
        #   'file:////home/...' -> '/home/...'
        #    there should be a function
        first_letter = input_file.split('/')[0]
        if (first_letter == "file"):
            input_file = input_file[9:]

        with open(input_file, "r") as in_file:
            for line in in_file:

                if (DEBUG):


                col_count_cur = len(line.split(' '))
                # Assumption first row is matched with header
                #   no None or NULL value in first row
                if (line_idx != 0):
                    if (col_count_cur == col_count_pre):
                        first_row = line
                        break
                col_count_pre = col_count_cur
                line_idx += 1
                header = line

        return {"no of col": col_count_cur,\
            "header": header, \
            "first_row": first_row,
            "line index of header": line_idx}

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

            else:
                pass

    # print out label with most commone contents under the label
    def content_guess(self, rdd_external = None):
        if (not rdd_external):
            rdd_external.countByValue()
        else:
            self.rdd.keys()
            self.rdd.countByValue()

        # find max occurance in each key
    def table_size(self, rdd_external = None):
        no_key, no_row = None, None
        if (not rdd_external):
            no_key = len(rdd_external.keys().collect())
            no_row = rdd_external.count()
        else:
            no_key = len(self.rdd.keys().collect())
            no_row = self.rdd.count()
        return {"total label": no_key, "no of row": no_row}

# Aggreate
class AnalysisDesk(PrepDesk):
    def __init__(self, dataset=None):
        #
        # Error spot
        #   ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=pyspark-shell, master=local[*]) created by __init__ at /home/cloud/computer_programming/python/common_merge/util.py:37
        #
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

    # similar to content_guess function
    #   group by same column contents in one (specific) label
    #     how choos label? least number of single member groups
    def find_group(self):
        # Call other function
        #   ref: https://stackoverflow.com/questions/63514386/use-of-self-when-calling-functions-within-a-class
        super.content_guess(self.rdd_external)
