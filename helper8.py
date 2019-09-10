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
    if row == None:
        return None
    else:
        if row[0] != 'HistoricalReservationID':
            try:
                return [int(row[0]), row[1], row[2], row[3], row[4], row[5], float(row[6]),float(row[7]), row[8], row[9], int(row[10])]
            except:
                return None
        else:
            return None 

    
def state_filter(row):
    if row == None:
        return row
    else:
        if row[5] == 'CO':
            return row
        else:
            return None
        
def cust_country_filter(row):
    if row == None:
        return row
    else:
        if row[9] == 'USA':
            return row
        else:
            return None

def clean_zips(row):
    if row == None:
        return row
    else:
        row[8] = row[8].split('-')[0]
        return row

def five_zips(row):
    if row == None:
        return row
    else:
        if len(row[8]) != 5:
            return None
        else:
            return row
    

def add_coords(row):
    if row == None:
        return row
    else:
        search = SearchEngine(simple_zipcode=True)
        simple_zipcode = search.by_zipcode(str(row[8]))
        lat = simple_zipcode.to_dict()['lat']
        lng = simple_zipcode.to_dict()['lng']
        row.append(lat)
        row.append(lng)
        return row

def add_census(row):
    if row == None:
        return row
    else:
        result = cg.coordinates(row[12], row[11])
        tract = result['2010 Census Blocks'][0]['TRACT']
        state = result['2010 Census Blocks'][0]['STATE']
        county = result['2010 Census Blocks'][0]['COUNTY']
        row.append(tract)
        row.append(state)
        row.append(county)
        return row