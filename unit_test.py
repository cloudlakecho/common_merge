
# unit_test.py
#

import argparse
import os, pdb, sys
if (int(sys.version_info.major) > 3) or \
    ((int(sys.version_info.major) == 3) and \
    (int(sys.version_info.minor) >= 8)):
    import Pathlib  # Python 3.8 and later
import util
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from util import PrepDesk, AnalysisDesk


def parse_args():
    parser = argparse.ArgumentParser(description='Unit Test')
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--function", type=str)  # if "all" it will run all unit test

    args = parser.parse_args()

    return args




# To find top saving in last quarter
def test_find_customer():
    month_by_quarter = [('01', '02', '03'), ('04', '05', '06'),
        ('07', '08', '09'), ('10', '11', '12')]
    search_team_desk = AnalysisDesk()

    rdd = search_team_desk.rdd
    if "date" in rdd.collect():
        lable = None
        # find last date, cheek quarter, one quarter down
        latest_date = rdd.groupByKey().mapValue(dict)[lable].max()
        # September: 09 -> 3 -> 2, so last quarter is second quarter
        last_quarter = int(latest_date.month / 3 - 1)
        month = month_by_quarter[last_quarter + 1]


# To read file and check nested and flatten them
def flatten():
    args = parse_args()
    # ETL: Extract, Transform, Load
    transform_team_desk = PrepDesk(app_name="test")
    rdd = transform_team_desk.load_file(args.input_file)
    rdd.keys().collect()

    pdb.set_trace()


def main():

    args = parse_args()
    if (args.function == "all"):
        test_find_customer()
        flatten()
    elif (args.function == "find customer"):
        test_find_customer()
    elif (args.function == "flatten"):
        flatten()
    else:
        print ("Please, choose among the choices.")



if __name__ == '__main__':
    main()
