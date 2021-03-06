import pyspark as ps    # for the pyspark suite
import requests
from bs4 import BeautifulSoup
import numpy as np
import censusgeocode as cg 
import csv
import json
import ast

def read_variable_names(file_name):
    with open(file_name, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        # get header from first row
        headers = next(reader)
        # get all the rows as a list
        data = list(reader)
        return np.array(data)



def add_census(row):
    if row[0] == 'CustZIP':
        return None
    else:
        try:
            census_geocode_dict = cg.coordinates(row[3], row[2])
            row.append(census_geocode_dict['2010 Census Blocks'][0]['TRACT'])
            row.append(census_geocode_dict['2010 Census Blocks'][0]['STATE'])
            row.append(census_geocode_dict['2010 Census Blocks'][0]['COUNTY'])
            return row
        except:
            return None 
    
# def parse_row_string(row):


def add_census_vars(row, var_names):
    if row == None:
        return row
    else:
        for i in range(len(var_names)):
            search_term = var_names[i][0]
            try:
                row.append(call_api(search_term, row))
            except:
                row.append(None)
        return row



def call_api(search_term, row, key="2f321eb597c3d3e59dfa9aa2f694622639dee6fc"):
    tract, state, county = row[5], row[6], row[7]
    query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=tract:{}&in=state:{}%20county:{}&key={}".format(search_term, tract, state, county, key)
    call = requests.get(query).text
    clean_call = ast.literal_eval(call)
    try:
        isolated_value =  float(clean_call[1][1])
        return isolated_value
    except:
        return None 



# if __name__ == '__main__':
    # row = '1007,MA,42.3,-72.4,1'
    # rows = add_census(row)

    # econ_var_names = read_variable_names('econ_var_names.csv')
    
    # ## Census Variable Names
    # # url = requests.get("https://api.census.gov/data/2017/acs/acs5/profile/variables.html").text
    # # soup = BeautifulSoup(url, "html.parser")
    # # table = soup.find('table')
    # # census_var_names = parse_table_to_data(table)[:, 0:2]

    # # search_term = econ_var_names[0][0]
    # # called = call_api(search_term, rows)
    # called = add_census_vars(econ_var_names, rows)



