# import pyspark as ps    # for the pyspark suite
import requests
from bs4 import BeautifulSoup
import numpy as np
import censusgeocode as cg 
import pandas as pd
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

def cluster_variables(all_variables, subset):
    return np.array([all_variables[i].tolist() for i in range(subset[0], subset[1]+1)])

def command_center(national, row, var_names):
    results = []
    if national:
        for i in range(len(var_names)):
            search_term = var_names[i][0]
            results.append([var_names[i][1], (national_call_api(search_term))])
        return results
    else:    
        if row == None:
            return row
        else:
            for i in range(len(var_names)):
                search_term = var_names[i][0]
                results.append((i, var_names[i][1], (call_api(search_term, row))))
            return results




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

def national_call_api(search_term, key="2f321eb597c3d3e59dfa9aa2f694622639dee6fc"):
    query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=us:1".format(search_term)
    call = requests.get(query).text
    clean_call = ast.literal_eval(call)
    isolated_value =  float(clean_call[1][1])
    return isolated_value
  



if __name__ == '__main__':
    # row = '1007,MA,42.3,-72.4,1'
    # rows = add_census(row)
    # tract, state, county = rows[5], rows[6], 15
    # key="2f321eb597c3d3e59dfa9aa2f694622639dee6fc"
    # search_term = 'DP03_0025E'
    # query = "https:;//api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=tract:{}&in=state:{}%20county:{}&key={}".format(search_term, tract, state, county, key)
    # call = requests.get(query).text

    econ_var_names = read_variable_names('data/econ_var_names.csv')
    econ_clusters = {'Industry': (31, 43), 'Commute': (17, 22), 'Income_Benefits': (50, 59), 'Health_Insurance': (94, 96)}
    industry = cluster_variables(econ_var_names, econ_clusters['Industry'])
    health = cluster_variables(econ_var_names, econ_clusters['Health_Insurance'])
    commute = cluster_variables(econ_var_names, econ_clusters['Commute'])
    income_benefits = cluster_variables(econ_var_names, econ_clusters['Income_Benefits'])

    # ## Census Variable Names
    # # url = requests.get("https://api.census.gov/data/2017/acs/acs5/profile/variables.html").text
    # # soup = BeautifulSoup(url, "html.parser")
    # # table = soup.find('table')
    # # census_var_names = parse_table_to_data(table)[:, 0:2]

    # search_term = econ_var_names[0][0]
    row = [1007, 'MA', 42.3, -72.4, 1, '820204', '25', '015']
    # called = call_api(search_term, rows)

    called = command_center(False, row, commute)
    national_health = command_center(True, 0, health)
    national_industry = command_center(True, 0, industry)
    national_commute = command_center(True, 0, commute)
    national_income_benefits = command_center(True, 0, income_benefits)
    national_data = np.array(national_health + national_industry + national_commute + national_income_benefits)
    pd.DataFrame(national_data).to_csv("data/national_data.csv")
    # search_term = 'DP03_0096PE'
    # call = national_call_api(search_term)



