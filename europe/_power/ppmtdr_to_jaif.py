# Convert Power Plant Matching (ppm) and Technology Data Repository (tdr) to the Juha drive Alvaro's Intermediate data Format (jaif) for use in the data pipelines of the energy modelling workbench
# ppm: Capacity, Efficiency and lifetime (DateOut-DateIn or DateOut-2020)
# tdr: 2020-2050, investment, FOM, efficiency

"""
To Do:
- [ ] CC and H2
- [ ] storage parameters
- [ ] check datavalues (e.g. kW vs MW)

Optional:
- [ ] Currently, for some parameters that only require 1 value in jaif, new units use the first milestoneyear for its value  while in some instances it probably should use the average over the years.
- [ ] aggregate all units by type and use them as another data (arche)type (probably requires moving loading of files from existin/new units to main file)
"""

import sys
import csv
import random
from pprint import pprint
from copy import deepcopy
from math import sqrt
from pprint import pprint
import geopandas as gpd
from shapely.geometry import Point
from fuzzywuzzy.process import extractOne
import spinedb_api as api

def main(geo,inf,rfy,msy,ppm,spd,
    geolevel="PECD1",#"PECD1",# "PECD2",# "NUTS2",# "NUTS3",#
    referenceyear="y2025",
    units_existing=['bioST','CCGT',"nuclear-3","oil-eng","SCPC","wasteST","geothermal"],
    units_new=['bioST','bioST+CC','CCGT',"CCGT+CC","CCGT-H2",'fuelcell',"geothermal","nuclear-3","OCGT","OCGT+CC","oil-eng","SCPC","SCPC+CC","wasteST"],#"hydro-turbine"
    units_CC=[],
    units_H2=[],
    commodities=[],
    #parameters_existing=["conversion_rate","operational_cost","capacity"],
    #parameters_new=["lifetime","investment_cost","fixed_cost","operational_cost","CO2_captured","conversion_rate"],
):
    #initialise jaif
    jaif = { # dictionary for intermediate data format
        "entities":[
            [
                "commodity",
                "elec",
                None
            ]
        ],
        "parameter_values":[]
    }

    #load data
    geomap = gpd.read_file(geo)
    geomap = geomap[geomap["level"]==geolevel]#used by units existing
    regions = geomap["id"].to_list()#used by units new
    
    #format data
    existing_units(jaif,inf,rfy,ppm,geomap,referenceyear,list(msy.keys()),units_existing,commodities)
    new_units(jaif,msy,regions,units_new,commodities)

    # save to spine database
    with api.DatabaseMapping(spd) as target_db:
        # empty database except for intermediary format and alternatives
        target_db.purge_items('parameter_value')
        target_db.purge_items('entity')
        target_db.refresh_session()
        target_db.commit_session("Purged entities and parameter values")
        importlog = api.import_data(target_db, **jaif)
        target_db.refresh_session()
        target_db.commit_session("Added pypsa data")
    return importlog

def existing_units(jaif,inf,rfy,ppm,geomap,referenceyear,milestoneyears,units_existing,commodities):
    """
    Adds existing units to jaif

    with parameters:
        conversion rate of 2025
        operational cost of 2025
        capacity of 2025
        technology__region : units_existing = expected capacity for y2030, y2040, y2050 based on decommissions
        technonology__to_commodity: capacity = 1.0
    """
    # load data
    yearly_inflation={}
    with open(inf,'r') as file:
        csvreader = csv.reader(file)
        next(csvreader)
        for line in csvreader:# next to skip header
            yearly_inflation[int(line[1])]=float(line[2])/100
    #print(yearly_inflation)#debugline

    #load reference year data and convert to dictionary
    unit_types={}
    #could be done differently as unit_types[line[0]][line[1]][year][line[2]]
    for year,path in rfy.items():#only one entry
        datayear=year
        with open(path, 'r') as file:
            unit_types[year]={}
            for line in csv.reader(file):
                line = map_tdr_jaif(line)
                if (line[0] in units_existing or line[0] in commodities or line[0]=='CC') and line[2]!="unknown":
                    if line[0] not in unit_types[year]:#to avoid stray entries, use fuzzy search of unit_types keys
                        unit_types[year][line[0]]={}
                    unit_types[year][line[0]][line[1]]=line[2]
                    #unit_types[year][line[0]][line[1]+'_description']=line[3]+' '+line[4]+' '+line[5]
    #print(unit_types)#debugline

    with open(ppm, mode='r') as file:
        unit_instances = list(csv.DictReader(file))
    #print(unit_instances)#debugline
    #aggregate and clean units
    unit_instances = aggregate_units(unit_instances, unit_types, units_existing, referenceyear, datayear, milestoneyears, geomap)
    #pprint(unit_instances)#debugline
    #pprint(unit_types)

    regionlist = []
    commoditylist = []
    technologylist = []
    #unit_type_key_list = [] # for debugging
    years = [referenceyear].extend(milestoneyears)
    for unit in unit_instances:
        #print([unit["region"],unit["commodity"],unit["technology"]])#debugline
        if unit["region"] not in regionlist:
            regionlist.append(unit["region"])
            jaif["entities"].extend([
                [
                    "region",
                    unit["region"],
                    None
                ],
            ])
        if unit["commodity"]:
            # commodity
            if unit["commodity"] not in commoditylist:
                commoditylist.append(unit["commodity"])
                jaif["entities"].append([
                    "commodity",
                    unit["commodity"],
                    None
                ])
        # power plant
        if unit["entityclass"]=="PP":
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                jaif["entities"].extend([
                    [
                        "technology",
                        unit["technology"]+"-existing",
                        None
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        None
                    ],
                ])
                if unit["commodity"]:
                    jaif["entities"].extend([
                        [
                            "commodity__to_technology",
                            [
                                unit["commodity"],
                                unit["technology"]+"-existing"
                            ],
                            None
                        ],
                        [
                            "commodity__to_technology__to_commodity",
                            [
                                unit["commodity"],
                                unit["technology"]+"-existing",
                                "elec"
                            ],
                            None
                        ],
                    ])
                jaif["parameter_values"].extend([
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "capacity",
                        1.0,
                        "Base"
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "fixed_cost",
                        search_data(unit, unit_types, unit["technology"], [datayear], "fixed_cost",modifier=inflationfactor(yearly_inflation,datayear,referenceyear)),
                        "Base"
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "operational_cost",
                        search_data(unit, unit_types, unit["technology"], [datayear], "operational_cost",modifier=inflationfactor(yearly_inflation,datayear,referenceyear)),
                        "Base"
                    ],
                ])
                if unit["commodity"]:
                    jaif["parameter_values"].extend([
                        [
                            "commodity__to_technology__to_commodity",
                            [
                                unit["commodity"],
                                unit["technology"]+"-existing",
                                "elec"
                            ],
                            "conversion_rate",
                            search_data(unit, unit_types, unit["technology"], [datayear], "conversion_rate"),
                            "Base"
                        ],
                        [
                            "commodity__to_technology__to_commodity",
                            [
                                unit["commodity"],
                                unit["technology"]+"-existing",
                                "elec"
                            ],
                            "CO2_captured",
                            search_data(unit, unit_types, unit["technology"], [datayear], "CO2_captured"),
                            "Base"
                        ],
                    ])
            jaif["entities"].extend([
                [
                    "technology__region",
                    [
                        unit["technology"]+"-existing",
                        unit["region"]
                    ],
                    None
                ],
            ])
            jaif["parameter_values"].extend([
                [
                    "technology__region",
                    [
                        unit["technology"]+"-existing",
                        unit["region"]
                    ],
                    "units_existing",
                    search_data(unit, unit_types, unit["technology"], years, "capacity", data = [[year,unit["capacity"][year]] for year in milestoneyears]),
                    "Base"
                ],
            ])
            #pprint(year_data(unit, unit_types,unit_types_key, "efficiency"))
        #if unit["entityclass"]=="CHP": # skip
        # storage
        if unit["entityclass"]=="Store":
            #map_tdr needs to be updated with storage bicharging and storage and so does this part
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                jaif["entities"].extend([
                    [
                        "storage",
                        unit["technology"]+"-existing",
                        None
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        None
                    ],
                ])
                jaif["parameter_values"].extend([
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "efficiency_in",
                        search_data(unit, unit_types, unit["technology"], [datayear], "efficiency", modifier=1/sqrt(2)),
                        "Base"
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "efficiency_out",
                        search_data(unit, unit_types, unit["technology"], [datayear], "efficiency", modifier=1/sqrt(2)),
                        "Base"
                    ],
                ])
                """
                jaif["parameter_values"].extend([
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "investment_cost",
                        search_data(unit, unit_types, unit["Technolgy"], years, "investment_cost"),
                        "Base"
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "fixed_cost",
                        search_data(unit, unit_types, unit["Technolgy"], years, "fixed_cost"),
                        "Base"
                    ],
                ])
                """
            jaif["entities"].extend([
                [
                    "storage__region",
                    [
                        unit["technology"]+"-existing",
                        unit["region"]
                    ],
                    None
                ],
            ])
            jaif["parameter_values"].extend([
                [
                    "storage__region",
                    [
                        unit["technology"]+"-existing",
                        unit["region"]
                    ],
                    "storages_existing",
                    search_data(unit, unit_types, unit["technology"],[referenceyear], "capacity", data=[[k,v] for k,v in unit["capacity"].items()]),
                    "Base"
                ],
            ])
    
    return jaif

def new_units(jaif,msy,regions,units_new,commodities):
    """
    Adds new units to jaif

    with parameters:
        lifetime
        investment_cost map for y2030, y2040, y2050
        fixed_cost map for y2030, y2040, y2050
        operational_cost map for y2030, y2040, y2050
        average CO2_captured
        average conversion_rate
        capacity = 1 from asset to main commodity
    """
    unit_types={}
    #could be done differently as unit_types[line[0]][line[1]][year][line[2]]
    for year,path in msy.items():#only one entry
        with open(path, 'r') as file:
            unit_types[year]={}
            for line in csv.reader(file):
                line = map_tdr_jaif(line)
                if (line[0] in units_new or line[0] in commodities or line[0]=='CC') and line[2]!="unknown":
                    if line[0] not in unit_types[year]:#to avoid stray entries, use fuzzy search of unit_types keys
                        unit_types[year][line[0]]={}
                    unit_types[year][line[0]][line[1]]=line[2]
                    #unit_types[year][line[0]][line[1]+'_description']=line[3]+' '+line[4]+' '+line[5]
    #print(unit_types)#debugline

    unit_instances = generate_unit_instances(regions,units_new)

    regionlist = []
    commoditylist = []
    technologylist = []
    #unit_type_key_list = [] # for debugging
    years = list(msy.keys())
    for unit in unit_instances:
        #print([unit["region"],unit["commodity"],unit["technology"]])#debugline
        if unit["region"] not in regionlist:
            regionlist.append(unit["region"])
            jaif["entities"].extend([
                [
                    "region",
                    unit["region"],
                    None
                ],
            ])
        if unit["commodity"]:
            # commodity
            if unit["commodity"] not in commoditylist:
                commoditylist.append(unit["commodity"])
                jaif["entities"].append([
                    "commodity",
                    unit["commodity"],
                    None
                ])
        # power plant
        if unit["entityclass"]=="PP":
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                jaif["entities"].extend([
                    [
                        "technology",
                        unit["technology"],
                        None
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        None
                    ],
                ])
                if unit["commodity"]:
                    jaif["entities"].extend([
                        [
                            "commodity__to_technology",
                            [
                                unit["commodity"],
                                unit["technology"]
                            ],
                            None
                        ],
                        [
                            "commodity__to_technology__to_commodity",
                            [
                                unit["commodity"],
                                unit["technology"],
                                "elec"
                            ],
                            None
                        ],
                    ])
                jaif["parameter_values"].extend([
                    [
                        "technology",
                        unit["technology"],
                        "lifetime",
                        search_data(unit, unit_types, unit["technology"], [years[0]], "lifetime"),
                        "Base"
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "capacity",
                        1.0,
                        "Base"
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "investment_cost",
                        search_data(unit, unit_types, unit["technology"], years, "investment_cost"),
                        "Base"
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "fixed_cost",
                        search_data(unit, unit_types, unit["technology"], years, "fixed_cost"),
                        "Base"
                    ],
                    [
                        "technology__to_commodity",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "operational_cost",
                        search_data(unit, unit_types, unit["technology"], years, "operational_cost"),
                        "Base"
                    ],
                ])
                if unit["commodity"]:
                    jaif["parameter_values"].extend([
                        [
                            "commodity__to_technology__to_commodity",
                            [
                                unit["commodity"],
                                unit["technology"],
                                "elec"
                            ],
                            "conversion_rate",
                            search_data(unit, unit_types, unit["technology"], [years[0]], "conversion_rate"),
                            "Base"
                        ],
                        [
                            "commodity__to_technology__to_commodity",
                            [
                                unit["commodity"],
                                unit["technology"],
                                "elec"
                            ],
                            "CO2_captured",
                            search_data(unit, unit_types, unit["technology"], [years[0]], "CO2_captured"),
                            "Base"
                        ],
                    ])
            jaif["entities"].extend([
                [
                    "technology__region",
                    [
                        unit["technology"],
                        unit["region"]
                    ],
                    None
                ],
            ])
            #pprint(year_data(unit, unit_types,unit_types_key, "efficiency"))
        #if unit["entityclass"]=="CHP": # skip
        # storage
        if unit["entityclass"]=="Store":
            #map_tdr needs to be updated with storage bicharging and storage and so does this part
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                jaif["entities"].extend([
                    [
                        "storage",
                        unit["technology"],
                        None
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        None
                    ],
                ])
                jaif["parameter_values"].extend([
                    [
                        "storage_connection",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "efficiency_in",
                        search_data(unit, unit_types, unit["technology"], [years[0]], "efficiency", modifier=1/sqrt(2)),
                        "Base"
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "efficiency_out",
                        search_data(unit, unit_types, unit["technology"], [years[0]], "efficiency", modifier=1/sqrt(2)),
                        "Base"
                    ],
                ])
                jaif["parameter_values"].extend([
                    [
                        "storage",
                        unit["technology"],
                        "lifetime",
                        search_data(unit, unit_types, unit["technology"], years, "lifetime"),
                        "Base"
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "investment_cost",
                        search_data(unit, unit_types, unit["technology"], years, "investment_cost"),
                        "Base"
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"],
                            "elec"
                        ],
                        "fixed_cost",
                        search_data(unit, unit_types, unit["Technolgy"], years, "fixed_cost"),
                        "Base"
                    ],
                ])
            jaif["entities"].extend([
                [
                    "storage__region",
                    [
                        unit["technology"],
                        unit["region"]
                    ],
                    None
                ],
            ])
    
    return jaif

def generate_unit_instances(regions,units):
    map_jaif={
        'bioST':{
            "commodity":"bio",
            "technology":"bioST",
            "entityclass":"PP",
        },
        'bioST+CC':{
            "commodity":"bio",
            "technology":"bioST+CC",
            "entityclass":"PP",
        },
        'CCGT':{
            "commodity":"CH4",
            "technology":"CCGT",
            "entityclass":"PP",
        },
        "CCGT+CC":{
            "commodity":"CH4",
            "technology":"CCGT+CC",
            "entityclass":"PP",
        },
        "CCGT-H2":{
            "commodity":"H2",
            "technology":"CCGT-H2",
            "entityclass":"PP",
        },
        'fuelcell':{
            "commodity":"H2",
            "technology":"fuelcell",
            "entityclass":"PP",
        },
        "geothermal":{
            "commodity":None,
            "technology":"geothermal",
            "entityclass":"PP",
        },
        "nuclear-3":{
            "commodity":"U-92",
            "technology":"nuclear-3",
            "entityclass":"PP",
        },
        "OCGT":{
            "commodity":"CH4",
            "technology":"OCGT",
            "entityclass":"PP",
        },
        "OCGT+CC":{
            "commodity":"CH4",
            "technology":"OCGT+CC",
            "entityclass":"PP",
        },
        "oil-eng":{
            "commodity":"HC",
            "technology":"oil-eng",
            "entityclass":"PP",
        },
        "SCPC":{
            "commodity":"coal",
            "technology":"SCPC",
            "entityclass":"PP",
        },
        "SCPC+CC":{
            "commodity":"coal",
            "technology":"SCPC+CC",
            "entityclass":"PP",
        },
    }
    unit_instances = []
    for region in regions:
        for unit in units:
            if unit in map_jaif:
                unit_jaif = map_jaif[unit]
                unit_jaif["region"]=region
                unit_instances.append(unit_jaif)
    return unit_instances

def aggregate_units(unit_instances, unit_types, units, referenceyear, datayear, milestoneyears, geomap,
    average_parameters = ["conversion_rate"],
    sum_parameters = [],
    cumulative_parameters = ["capacity"],
):
    """
    Aggregate and clean units
    """
    aggregated_units = {}
    for unit in unit_instances:
        unit = map_ppm_jaif(unit)
        if unit["technology"] in units:
            #clean parameter fields
            unit["region"]=get_region(unit,geomap)
            #tuple for aggregating
            unit_tuple = tuple([unit[key] for key in ["commodity","technology","region"]])
            if unit_tuple not in aggregated_units.keys():
                #print(unit_tuple)# debug and information line
                #initialise
                aggregated_units[unit_tuple] = deepcopy(unit)
                aggregated_unit = aggregated_units[unit_tuple]
                for parameter in average_parameters:
                    unit[parameter]=search_data(unit,unit_types,unit["technology"],[datayear],parameter)
                    if unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
                for parameter in sum_parameters:
                    unit[parameter]=search_data(unit,unit_types,unit["technology"],[datayear],parameter)
                    if unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
                for parameter in cumulative_parameters:
                    if parameter == "capacity":
                        lifetime = search_data(unit,unit_types,unit["technology"],[datayear],"lifetime")
                        unit["capacity"] = decay_capacity(unit, lifetime, referenceyear, milestoneyears)
                    if unit[parameter]:
                        aggregated_unit[parameter] = deepcopy(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
            else:
                #aggregate
                aggregated_unit = aggregated_units[unit_tuple]
                for parameter in average_parameters:
                    unit[parameter]=search_data(unit,unit_types,unit["technology"],[datayear],parameter)
                    if aggregated_unit[parameter] and unit[parameter]:
                        aggregated_unit[parameter] = (float(aggregated_unit[parameter]) + float(unit[parameter]))/2
                    elif unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                for parameter in sum_parameters:
                    unit[parameter]=search_data(unit,unit_types,unit["technology"],[datayear],parameter)
                    if aggregated_unit[parameter] and unit[parameter]:
                        aggregated_unit[parameter] = float(aggregated_unit[parameter]) + float(unit[parameter])
                    elif unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                for parameter in cumulative_parameters:
                    if parameter == "capacity":
                        lifetime=search_data(unit,unit_types,unit["technology"],[datayear],"lifetime")
                        unit["capacity"]=decay_capacity(unit, lifetime, referenceyear, milestoneyears)
                    #else assume data is already in correct format
                    if aggregated_unit[parameter] and unit[parameter]:
                        for year in aggregated_unit[parameter].keys():
                            aggregated_unit[parameter][year]+=unit[parameter][year]
                    elif unit[parameter]:
                        aggregated_unit[parameter]=deepcopy(unit[parameter])
    return aggregated_units.values()

def get_region(unit,geomap):
    lat = float(unit["lat"])
    lon = float(unit["lon"])
    point = Point(lon,lat)
    poly_index = geomap.distance(point).sort_values().index[0]
    poly = geomap.loc[poly_index]
    region = poly["id"]
    #print(unit["region"])#debugline
    #print(region)#debugline
    return region

def decay_capacity(unit,lifetime,referenceyear,milestoneyears):
    capacity = {referenceyear:float(unit["capacity"])}
    # try to use dateout
    if unit["date_out"]:
        for milestoneyear in milestoneyears:
            if int(milestoneyear[1:]) < int(float(unit["date_out"])):
                capacity[milestoneyear]=float(unit["capacity"])
            else:
                capacity[milestoneyear]=0.0
    elif unit["date_in"] and lifetime:
        for milestoneyear in milestoneyears:
            if int(milestoneyear[1:]) < int(float(unit["DateIn"]))+int(float(lifetime)):
                capacity[milestoneyear]=float(unit["capacity"])
            else:
                capacity[milestoneyear]=0.0
    else:
        randomyear = random.choice(milestoneyears)
        for milestoneyear in milestoneyears:
            if int(milestoneyear[1:]) < int(randomyear[1:]):
                capacity[milestoneyear]=float(unit["capacity"])
            else:
                capacity[milestoneyear]=0.0
    return capacity

def map_ppm_jaif(unit_ppm):
    map_ppm={#print and copy all possible tuples in ppm (from debug script) and then make a manual map to jaif
        #(fuel,tech,set)
        ('Hard Coal', 'Steam Turbine', 'PP'):("coal","SCPC","PP"),
        ('Nuclear', 'Steam Turbine', 'PP'):("U-92","nuclear-3","PP"),
        #('Hard Coal', 'Steam Turbine', 'CHP'):("","",""),
        #('Hydro', 'Reservoir', 'Store'):("","",""),
        #('Hydro', 'Run-Of-River', 'Store'):("","",""),
        #('Hydro', 'Pumped Storage', 'Store'):("","",""),
        #('Hydro', 'Run-Of-River', 'PP'):("","",""),
        #('Hard Coal', 'CCGT', 'CHP'):("","",""),
        ('Hard Coal', 'CCGT', 'PP'):("coal","SCPC","PP"),#"CCGT"
        ('Lignite', 'Steam Turbine', 'PP'):("coal","SCPC","PP"),
        #('Natural Gas', 'CCGT', 'CHP'):("","",""),
        ('Natural Gas', 'CCGT', 'PP'):("CH4","CCGT","PP"),
        ('Solid Biomass', 'Steam Turbine', 'PP'):("bio","bioST","PP"),
        #('Lignite', 'Steam Turbine', 'CHP'):("","",""),
        #('Oil', 'Steam Turbine', 'CHP'):("","",""),
        #('Hydro', 'Reservoir', 'PP'):("","",""),
        ('Oil', 'Steam Turbine', 'PP'):("HC","oil-eng","PP"),
        #('Oil', 'CCGT', 'CHP'):("","",""),
        #('Lignite', 'CCGT', 'CHP'):("","",""),
        ('Natural Gas', 'Steam Turbine', 'PP'):("CH4","CCGT","PP"),
        ('Hard Coal', None, 'PP'):("coal","SCPC","PP"),
        (None, 'Steam Turbine', 'PP'):("CH4","CCGT","PP"),
        #('Natural Gas', 'Steam Turbine', 'CHP'):("","",""),
        #(None, 'Steam Turbine', 'CHP'):("","",""),
        #('Hydro', None, 'PP'):("","",""),
        #('Solar', 'Pv', 'CHP'):("","",""),
        #('Hydro', None, 'Store'):("","",""),
        #('Wind', 'Onshore', 'PP'):("","",""),
        #(None, 'Marine', 'Store'):("","",""),
        #('Wind', 'Offshore', 'PP'):("","",""),
        ('Lignite', None, 'PP'):("coal","SCPC","PP"),
        ('Geothermal', 'Steam Turbine', 'PP'):(None,"geothermal","PP"),
        #('Hydro', 'Pumped Storage', 'PP'):("","",""),
        #('Wind', 'Onshore', 'Store'):("","",""),
        #('Solar', 'Pv', 'PP'):("","",""),
        #('Solid Biomass', 'CCGT', 'CHP'):("","",""),
        (None, 'CCGT', 'PP'):("CH4","CCGT","PP"),
        #('Solid Biomass', 'Steam Turbine', 'CHP'):("","",""),
        ('Oil', None, 'PP'):("HC","oil-eng","PP"),
        #('Hard Coal', None, 'CHP'):("","",""),
        #('Hydro', 'Run-Of-River', 'CHP'):("","",""),
        ('Waste', None, 'PP'):("waste","wasteST","PP"),
        ('Waste', 'Steam Turbine', 'PP'):("waste","wasteST","PP"),
        ('Oil', 'CCGT', 'PP'):("HC","oil-eng","PP"),
        ('Biogas', None, 'PP'):("bio","bioST","PP"),
        ('Biogas', 'CCGT', 'PP'):("bio","bioST","PP"),
        (None, None, 'PP'):("CH4","CCGT","PP"),
        ('Natural Gas', None, 'PP'):("CH4","CCGT","PP"),
        #('Natural Gas', None, 'CHP'):("","",""),
        ('Solid Biomass', None, 'PP'):("bio","bioST","PP"),
        #('Waste', 'Steam Turbine', 'CHP'):("","",""),
        #('Solar', 'Pv', 'Store'):("","",""),
        ('Waste', 'CCGT', 'PP'):("waste","wasteST","PP"),
        #('Wind', None, 'PP'):("","",""),
        ('Solid Biomass', 'Pv', 'PP'):("bio","bioST","PP"),#assumption
        ('Geothermal', None, 'PP'):(None,"geothermal","PP"),
        #('Biogas', 'Steam Turbine', 'CHP'):("","",""),
        #(None, None, 'Store'):("","",""),
        #('Natural Gas', 'Combustion Engine', 'CHP'):("","",""),
        ('Biogas', 'Steam Turbine', 'PP'):("bio","bioST","PP"),
        #(None, None, 'CHP'):("","",""),
        #('Oil', None, 'CHP'):",
        ('Natural Gas', 'Combustion Engine', 'PP'):("CH4","CCGT","PP"),#assumption
        ('Biogas', 'Combustion Engine', 'PP'):("bio","bioST","PP"),#assumption
        #('Waste', None, 'CHP'):("","",""),
        #('Solar', 'PV', 'PP'):("","",""),
        #('Solar', 'CSP', 'PP'):("","",""),
    }
    unknown=['Other'," ","","unknown","Unknown","not found","Not Found"]
    if unit_ppm["Technology"] in unknown:
        tech = None
    else:
        tech = unit_ppm["Technology"]
    if unit_ppm["Fueltype"] in unknown:
        fuel = None
    else:
        fuel = unit_ppm["Fueltype"]
    (fuel_ppm,tech_ppm,set_ppm) = map_ppm.get((fuel,tech,unit_ppm["Set"]),("unknown","unknown","unknown"))
    try:
        eta_ppm=float(unit_ppm["Efficiency"])
    except:
        eta_ppm=None
    try:
        cap_ppm=float(unit_ppm["Capacity"])
    except:
        cap_ppm=None
    try:
        datein_ppm=float("DateIn")
    except:
        datein_ppm=None
    try:
        dateout_ppm=float("DateOut")
    except:
        dateout_ppm=None
    unit_jaif = {
        "commodity":fuel_ppm,
        "technology":tech_ppm,
        "entityclass":set_ppm,
        "conversion_rate":eta_ppm,
        "capacity":cap_ppm,
        "date_in":datein_ppm,
        "date_out":dateout_ppm,
        "lat":unit_ppm["lat"],
        "lon":unit_ppm["lon"],
    }
    return unit_jaif

def map_tdr_jaif(line_tdr):
    #dictionary with name of technology in datafile to name in intermediate format
    map_tdr0={
        "biogas":"bioST",
        "biogas CC":"bioST+CC",
        #"biogas plus hydrogen":"bioST-H2"
        "CCGT":"CCGT",
        #"CCGT+CC",
        #"CCGT-H2",
        "fuel cell":"fuelcell",
        "geothermal":"geothermal",
        "pumped-Storage-Hydro-bicharger":"hydro-turbine",
        "nuclear":"nuclear-3",
        "OCGT":"OCGT",
        #"OCGT+CC",
        #"OCGT-H2",
        "oil":"oil-eng",
        "coal":"SCPC",
        #"SCPC+CC",
        "biomass":"wasteST",#assumption

        #"biogas":"bio",
        #"gas":"CH4",
        #"CO2",
        #"coal":"coal",
        #"elec",
        #"hydrogen":"H2",
        #"oil":"HC",
        #"uranium":"U-92",
        #"waste",

        "direct air capture":"CC",
    }
    map_tdr1={
        "FOM":"fixed_cost",
        "investment":"investment_cost",
        "lifetime":"lifetime",
        "VOM":"operational_cost",
        "efficiency":"conversion_rate",
        "C stored":"CO2_captured",
        "CO2 stored":"CO2_captured",
        #"capture rate":"CO2_capture_rate",
        #"capture_rate":"CO2_capture_rate",
        "capacity":"capacity",
        #"fuel":"operational_cost",
    }
    try:
        map_tdr2 = float(line_tdr[2])
    except:
        map_tdr2 = None
    
    line_jaif = [
        map_tdr0.get(line_tdr[0],"unknown"),
        map_tdr1.get(line_tdr[1],"unknown"),
        map_tdr2
    ]
    #print(f"replacing {line_tdr} for {line_jaif}")#debugline
    return line_jaif

def search_data(unit, unit_types, unit_type_key, years, parameter, data=None, modifier=1.0):
    if not data:
        data=[]
        for year in years:
            if unit_type_key in unit_types[year]:
                unit_type=unit_types[year][unit_type_key]
            else:
                unit_type={}
            datavalue = None
            if parameter in unit:
                if unit[parameter]:
                    datavalue = unit[parameter]
            if not datavalue and parameter in unit_type:
                if unit_type[parameter]:
                    datavalue = unit_type[parameter]
            if datavalue:
                datavalue*=modifier
            data.append([year,datavalue])
    if len(data)==0:
        parameter_value = None
        print(f"Cannot find parameter {parameter} for {unit["technology"]}")
    elif len(data) > 1:
        parameter_value = {
        "index_type": "str",
        "rank": 1,
        "index_name": "year",
        "type": "map",
        "data": data,
    }
    else:
        parameter_value = data[0][1]
    return parameter_value

def inflationfactor(yearly_inflation, year, referenceyear):
    if "y" in year:
        year = int(year[1:])
    if "y" in referenceyear:
        referenceyear = int(referenceyear[1:])
    inflation = 1.0
    for y in range(year,referenceyear):
        inflation *= 1-yearly_inflation[y]
    return inflation

if __name__ == "__main__":
    #flexibility in input
    #geo = sys.argv[1]
    #inf = sys.argv[2]
    #ppm = sys.argv[3] # pypsa power plant matching
    #tdr = {str(2020+(i-2)*10):sys.argv[i] for i in range(4,len(sys.argv)-1)} # pypsa technology data repository
    #spd = sys.argv[-1] # spine database preformatted with an intermediate format for the mopo project
    #flexibility in order (with limited flexibility of input)
    inputfiles={
        "geo":"geo",#"onshore.geojson",
        "inf":"inflation",#"EU_historical_inflation_ECB.csv",
        #"tdr":{"y2020":"costs_2020","y2030":"costs_2030","y2040":"costs_2040","y2050":"costs_2050",},
        "rfy":{"y2020":"costs_2020",},
        "msy":{"y2030":"costs_2030","y2040":"costs_2040","y2050":"costs_2050",},
        "ppm":"powerplants",#"powerplants.csv",
        "spd":"http",#spine db
    }
    for key,value in inputfiles.items():
        if type(value) == dict:
            for k,v in value.items():
                if extractOne(v,sys.argv[1:]):
                    inputfiles[key][k]=extractOne(v,sys.argv[1:])[0]
                else:
                    inputfiles[key][k]=None
                print(f"Using {inputfiles[key][k]} as {v}")
        else:
            if extractOne(value,sys.argv[1:]):
                inputfiles[key]=extractOne(value,sys.argv[1:])[0]
            else:
                inputfiles[key]=None
            print(f"Using {inputfiles[key]} as {value}")
    
    importlog = main(**inputfiles)
    pprint(importlog)# debug and information line