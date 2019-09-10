from uszipcode import SearchEngine
import censusgeocode as cg 
import requests
from bs4 import BeautifulSoup
import numpy as np

# census api key 
key = "2f321eb597c3d3e59dfa9aa2f694622639dee6fc"

## census variables

url = requests.get("https://api.census.gov/data/2017/acs/acs5/profile/variables.html").text
soup = BeautifulSoup(url, 'html.parser')
table = soup.find('table')

def table_to_data(table):
    data = []
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)
    return data

var_names = np.array(table_to_data(table))[:, 0:2]
search_term = var_names[38][0] 
search_term1 = var_names[72][0]

search = SearchEngine(simple_zipcode=False) # rich info database
search = SearchEngine(simple_zipcode=True) # simple info database
# zipcode = search.by_zipcode("32344") 
simple_zipcode = search.by_zipcode("32344")
coords = [simple_zipcode.to_dict()['lat'], simple_zipcode.to_dict()['lng']]

# coordinates to census tract 
result = cg.coordinates(coords[1], coords[0])
tract = result['2010 Census Blocks'][0]['TRACT']
state = result['2010 Census Blocks'][0]['STATE']
county = result['2010 Census Blocks'][0]['COUNTY']

# example census query 
query1 = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME," + search_term1 + "&for=tract:" + tract + "&in=state: " + state + " %20county:" + county + "&key=" + key
r = requests.get(query1).text



    


