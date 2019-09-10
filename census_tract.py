import pyspark as ps    # for the pyspark suite
import requests
from bs4 import BeautifulSoup
import numpy as np
import censusgeocode as cg 
import csv




def add_census(row):
    row_list = row.split(',')
    if row_list[0] == 'CustZIP':
        return None
    else:
        try:
            census_geocode_dict = cg.coordinates(row_list[3], row_list[2])
            row_list.append(census_geocode_dict['2010 Census Blocks'][0]['TRACT'])
            row_list.append(census_geocode_dict['2010 Census Blocks'][0]['STATE'])
            row_list.append(census_geocode_dict['2010 Census Blocks'][0]['COUNTY'])
            return row_list
        except:
            return None 