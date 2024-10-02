
# unit_test.py
#
#
# Runtime environment
#   vanguard
#
# How to run this code:
#   python unit_test.py --function <function> --in_file <input file if>
#   for example
#     image extraction
#       python unit_test.py --function "extract image" --in_folder "/home/cloud/Documents" --file_format pdf
#
#     Text file reading
#       python unit_test.py --function flatten --input_file ~/data/company/fintech\ list\ from\ growjo\ 10000.txt

#
# To do
#   please check
#
# Error
#   please check
#

import argparse
import os, pdb, sys
if (int(sys.version_info.major) > 3) or \
    ((int(sys.version_info.major) == 3) and \
    (int(sys.version_info.minor) >= 8)):
    import Pathlib  # Python 3.8 and later
import pytest

import util
import util_image

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


DEBUG = False


if (DEBUG):
    pdb.set_trace()

# check PySpark version and if higher than 3.5
[major, minor, _] = (pyspark.__version__).split('.')
if (int(major) > 3) or \
   (int(major) == 3 and int(minor) >= 5):
   import pyspark.errors.PySparkException


from util import PrepDesk, AnalysisDesk


def parse_args():
    parser = argparse.ArgumentParser(description='Unit Test')
    parser.add_argument("--input_file", type=str)
    parser.add_argument('--in_file', dest='in_file',
        help='input file name')
    parser.add_argument('--in_folder', dest='in_folder',
        help='input folder name')
    parser.add_argument('--file_format', dest='file_format',
        help='input/output file format')

    parser.add_argument('--out_file', dest='out_file',
        help='output file name')
    parser.add_argument('--out_folder', dest='out_folder',
        help='out folder name')
    parser.add_argument("--function", type=str)  # if "all" it will run all unit test
    parser.add_argument('--choice', dest='choice', type=str,
        help='task choice')

    args = parser.parse_args()

    return args


# To find top saving in last quarter
def test_find_customer():
    month_by_quarter = [('01', '02', '03'), ('04', '05', '06'),
        ('07', '08', '09'), ('10', '11', '12')]
    search_team_desk = AnalysisDesk()

    rdd = search_team_desk.rdd
    # please check data size using "count" before "collect"
    #   "collect" brings all data
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

    if (DEBUG):
        pdb.set_trace()

    # Filtering in selecting data
    #   I may skip exception handling
    #     there is way https://stackoverflow.com/a/61942701/5595995
    #     but it looks like needed version 3.5
    # with pytest.raises(Exception, match="SparkException"):
    #     all_key = rdd.keys().collect()
    # To do
    #   catch when there is only value dataset
    all_key = rdd.keys().collect()
    print ("All keys:", all_key)


# ----- ----- ----- -----
def main():

    args = parse_args()

    if (args.function == "all"):
        test_find_customer()
        flatten()
    elif (args.function == "find customer"):
        test_find_customer()
    elif (args.function == "flatten"):
        flatten()
    elif (args.function == "extract image"):
        util_image.extract_image(args)
    else:
        print ("Please, choose among the choices.")


if __name__ == '__main__':
    main()
