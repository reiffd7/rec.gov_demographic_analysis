# import pyspark as ps    # for the pyspark suite
import json  # for parsing json formatted data
from uszipcode import SearchEngine
import censusgeocode as cg 
import csv              # for the split_csvstring function from Part 3.2.2
try:                    # Python 3 compatibility
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import os 
import geopy.distance
import pgeocode
import math



def parse_rows(row):
    row_list = row.split(',')
    row_list = [i.replace('"', '') for i in row_list]
    try:
        return [row_list[0], row_list[8], row_list[10], row_list[11], row_list[12], row_list[18], row_list[19], row_list[20], row_list[21], row_list[23], row_list[33], row_list[22]]
    except:
        return None 
   

def caster(row):
    if row[0] != 'HistoricalReservationID':
        return int(row[0]), row[1], row[2], row[3], row[4], row[5], float(row[6]),float(row[7]), row[8], row[9], int(row[10]), row[11]
    else:
        return row

def state_filter(row):
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
        nomi = pgeocode.Nominatim('us')
        query = nomi.query_postal_code(str(row[8]))
        if math.isnan(round(query['latitude'], 2)):
            return None 
        else:
            row.append(round(query['latitude'], 2))
            row.append(round(query['longitude'], 2))

    # search = SearchEngine(simple_zipcode=True)
    # simple_zipcode = search.by_zipcode()
    # row.append(simple_zipcode.to_dict()['lat'])
    # row.append(simple_zipcode.to_dict()['lng'])
    return row

def distance(row):
    if row == None:
        return row
    else:
        fac_coords = (row[7], row[6])
        cust_coords = (row[12], row[13])
        dist = geopy.distance.vincenty(fac_coords, cust_coords).km
        row.append(dist)
        return row


def add_census(row):
    result = cg.coordinates(row[12], row[11])
    tract = result['2010 Census Blocks'][0]['TRACT']
    state = result['2010 Census Blocks'][0]['STATE']
    county = result['2010 Census Blocks'][0]['COUNTY']
    row.append(tract)
    row.append(state)
    row.append(county)
    return row


if __name__ == '__main__':
    row = [2976269875, 'Alaska Region','Klondike Gold Rush National Historical Park (home of Jeff. Smiths Parlor Museum - Skagway Historic District Walking Tours - Dyea Walking Tours)', 'Historic Tour','',-135.3138888888889,59.45833333333334,32344,'USA',2,30.4,-83.9]
    row1 = '"HistoricalReservationID","OrderNumber","Agency","OrgID","CodeHierarchy","RegionCode","RegionDescription","ParentLocationID","ParentLocation","LegacyFacilityID","Park","SiteType","UseType","ProductID","EntityType","EntityID","FacilityID","FacilityZIP","FacilityState","FacilityLongitude","FacilityLatitude","CustomerZIP","CustomerState","CustomerCountry","Tax","UseFee","TranFee","AttrFee","TotalBeforeTax","TotalPaid","StartDate","EndDate","OrderDate","NumberOfPeople","Tent","Popup","Trailer","RVMotorhome","Boat","HorseTrailer","Car","FifthWheel","Van","CanoeKayak","BoatTrailer","Motorcycle","Truck","Bus","Bicycle","Snowmobile","OffRoadlAllTerrainVehicle","PowerBoat","PickupCamper","LargeTentOver9x12","SmallTent","Marinaboat","LatLongPoint"'
    zipex = '75041-2111'
    faclong = -106.273056
    faclat = 37.072498
    fac_coords = (faclat, faclong)
    custlat = 38.919998
    custlong = -104.709999
    cust_coords = (custlat, custlong)
    dist = geopy.distance.vincenty(fac_coords, cust_coords).km
    search = SearchEngine(simple_zipcode=True)
    simple_zipcode = search.by_zipcode('1720')
    nomi = pgeocode.Nominatim('us')
    query = nomi.query_postal_code('80308')



#     parsed = parse_rows(row)
    parsed1 = parse_rows(row1)