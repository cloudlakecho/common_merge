
# unit_test.py
#

import argparse
import os, pdb, sys
import Pathlib  # Python 3.8 and later
import util



def parse_args():
    args = argparse("Title": , "Comment": "Unit test of function")
    args.add("--input_file", type=str)

    return args


# To find top saving in last quarter
def test_find_customer():
    month_by_quarter = [('01', '02', '03'), ('04', '05', '06'),
        ('07', '08', '09'), ('10', '11', '12')]
    search_team_desk = AnalysisDesk()
    search_team_dask.rdd = rdd
    if "date" in rdd.keys():
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
    
