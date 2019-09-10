import pyspark as ps    # for the pyspark suite
import json  # for parsing json formatted data
from uszipcode import SearchEngine
import censusgeocode as cg 
import csv              # for the split_csvstring function from Part 3.2.2
try:                    # Python 3 compatibility
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import os 


def parse_rows(row):
    row_list = row.split(',')
    row_list = [i.replace('"', '') for i in row_list]
    try:
        return [row_list[0], row_list[8], row_list[10], row_list[11], row_list[12], row_list[18], row_list[19], row_list[20], row_list[21], row_list[23], row_list[33]]
    except:
        return None 


def caster(row):
    if row[0] != 'HistoricalReservationID':
        return int(row[0]), row[1], row[2], row[3], row[4], row[5], float(row[6]),float(row[7]), row[8], row[9], int(row[10])
    else:
        return None 
    
def state_filter(row):
    if row[5] == 'CO':
        return row
    else:
        return None