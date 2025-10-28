import csv
import geopandas as gpd
from pprint import pprint
#from ppmtdr_to_jaif import get_region

geo = gpd.read_file('/home/u0102409/MyApps/spinetools/geo/onshore.geojson')
geo = geo[geo["level"]=="PECD1"] #level = PECD1, PECD2, NUTS2, NUTS3

data_path='/home/u0102409/MyApps/pypsa/pypsa-eur data/'
tdr={
    '2020':data_path+'costs_2020.csv',
    '2030':data_path+'costs_2030.csv',
    '2040':data_path+'costs_2040.csv',
    '2050':data_path+'costs_2050.csv',
}

unit_type_list=[]
unit_type_parameter_list=[]
unit_types={}
for year,path in tdr.items():
    with open(path, 'r') as file:
        unit_types[year]={}
        for line in csv.reader(file):
            if line[0] not in unit_type_list:
                unit_type_list.append(line[0])
            if line[1] not in unit_type_parameter_list:
                unit_type_parameter_list.append(line[1])
            if line[0] not in unit_types[year]:
                unit_types[year][line[0]]={}
            unit_types[year][line[0]][line[1]]=line[2]
            #unit_types[year][line[0]][line[1]+'_description']=line[3]+' '+line[4]+' '+line[5]
print("\n unit_types")
pprint(unit_type_list)
print("\n unit_type_parameters")
pprint(unit_type_parameter_list)

# check consistency
for unit_type in unit_types.values():
    for key in unit_type.keys():
        for year,unit_type_year in unit_types.items():
            if key not in unit_type_year.keys():
                print(key+' is not in '+year)

ppm=data_path+'powerplants.csv'
with open(ppm, mode='r') as file:
    unit_instances = list(csv.DictReader(file))
#pprint(unit_instances)
ppmkeys = ['\ufeffid', 'Name', 'Fueltype', 'Technology', 'Set', 'Country', 'Capacity', 'Efficiency', 'lat', 'lon']
exclude=['Other','Waste','Geothermal','hydro','Hydro','CHP','Reservoir', 'Run-Of-River', 'Pumped Storage', 'PV','Pv','CSP','Wind', 'Onshore', 'Offshore', 'Marine']
unitlist=[]
fuellist=[]
otherlist=[]
tuplelist=[]
clean = ["Other"," ","","unknown","Unknown","not found","Not Found"]
for unit in unit_instances:
    if unit["Fueltype"] in clean:
        unit["Fueltype"] = None
    if unit["Technology"] in clean:
        unit["Technology"] = None
    
    if unit["Fueltype"] not in fuellist:
        fuellist.append(unit["Fueltype"])
    if unit["Technology"] not in unitlist:
        unitlist.append(unit["Technology"])
    if (unit["Fueltype"],unit["Technology"],unit["Set"]) not in tuplelist:
        tuplelist.append((unit["Fueltype"],unit["Technology"],unit["Set"]))
print("\n unitlist in ppm")
pprint(unitlist)
print("\n otherlist in ppm")
pprint(otherlist)
print("\n fuellist in ppm")
pprint(fuellist)
print("\n tuplelist in ppm")
pprint(tuplelist)