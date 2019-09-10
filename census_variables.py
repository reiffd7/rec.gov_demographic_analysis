import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


def parse_table_to_data(table):
    data = []
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)
    return np.array(data)


def filter_subset_data(census_var_names, subset):
    econ_vars = census_var_names[census_var_names[:,2] == subset]
    return econ_vars

def filter_percentages(census_vars):
    result = []
    for i in range(len(census_vars)):
        if 'Percent Estimate' in econ_vars[i][1]:
            result.append([econ_vars[i][0], econ_vars[i][1]])
    return np.array(result)

def var_names_to_file(var_names, file_name):
    pd.DataFrame(var_names).to_csv(file_name, header=False, index=False)

if __name__ == '__main__':
    # row = '1007,MA,42.3,-72.4,1'
    # row_w_census = add_census(row)

    ## Census Variable Names
    url = requests.get("https://api.census.gov/data/2017/acs/acs5/profile/variables.html").text
    soup = BeautifulSoup(url, "html.parser")
    table = soup.find('table')
    census_var_names = parse_table_to_data(table)[:, 0:3]

    subset = 'SELECTED ECONOMIC CHARACTERISTICS'
    econ_vars = filter_subset_data(census_var_names, subset)
    econ_percent_vars = filter_percentages(econ_vars)

    var_names_to_file(econ_percent_vars, 'econ_var_names.csv')

