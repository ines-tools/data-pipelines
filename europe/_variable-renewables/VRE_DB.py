import spinedb_api as api
from spinedb_api import DatabaseMapping
import datetime
import pandas as pd
import sys
import numpy as np
import json

def add_entity(db_map : DatabaseMapping, class_name : str, entity_byname : str, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=entity_byname, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)
    
def time_index(year) -> list:
    time_list = {}
    pd_range = pd.date_range(str(int(year))+"-01-01 00:00:00",str(int(year))+"-12-31 23:00:00",freq="h")
    time_list["standard"] = [i.strftime('%Y-%m-%d %H:%M:%S') for i in pd_range if not (i.month==2 and i.day==29)]
    time_list["iso"]  = [i.isoformat() for i in pd_range if not (i.month==2 and i.day==29)]
    return time_list["standard"], time_list["iso"]

def read_excel_data(file_name, sheet_name, index_col, column):
    return pd.read_excel(file_name, sheet_name=sheet_name, index_col=index_col)[column]

def add_region(db_map, poly, region_type, gis_level):
    try:
        add_entity(db_map, "region", (poly,))
    except Exception as e:
        pass#print(f"Error adding region {poly}: {e}")

def add_technology_relationship(db_map, tech_type, tech, poly, potential, availability, CY_index):
    try:
        add_entity(db_map, "technology_type__region", (tech_type, poly))
        add_parameter_value(db_map, "technology_type__region", "potential", "Base", (tech_type, poly), round(float(potential) * 1e3, 1))
    except Exception as e:
        pass#print(f"Error adding technology relationship {tech_type} for {poly}: {e}")

    profile = {"type":"map","index_type":"date_time","index_name":"t","data":dict(zip(CY_index["iso"], availability.loc[CY_index["standard"], poly].round(3).tolist()))}
    # profile = {"type": "time_series", "data": availability.loc[CY_index["standard"], poly].round(3).tolist(), "index": {"start": "2018-01-01T00:00:00", "resolution": "1h", "ignore_year": True}}
    add_entity(db_map, "technology__to_commodity__region", (tech, "elec",poly))
    add_parameter_value(db_map, "technology__to_commodity__region", "profile_limit_upper", "Base", (tech, "elec",poly), profile)

def path_to_file(file_list,file):
    return [element for element in file_list if file in element][0]

def main():

    url_db_out = sys.argv[1]
    existing_wind_on = read_excel_data(path_to_file(sys.argv[1:],"capacity_wind-on-existing.xlsx"), "Regional", 0, 2025)
    existing_wind_off = read_excel_data(path_to_file(sys.argv[1:],"capacity_wind-off-existing.xlsx"), "Regional", 0, 2025)
    existing_solar_PV = read_excel_data(path_to_file(sys.argv[1:],"capacity_solar-PV-existing.xlsx"), "Regional", 0, 2025)
    potential_wind_on = read_excel_data(path_to_file(sys.argv[1:],"potential_wind-on.xlsx"), "Data", 0, "Greenfield_potential_GW")
    potential_wind_off = read_excel_data(path_to_file(sys.argv[1:],"potential_wind-off.xlsx"), "Bottom_fixed_max120kmFromShore", 0, "Greenfield_potential_GW")
    potential_solar_PV = read_excel_data(path_to_file(sys.argv[1:],"potential_solar-PV.xlsx"), "Data", 0, "Greenfield_potential_GW")

    # Read VRE costs
    vre_cost = pd.read_csv(path_to_file(sys.argv[1:],"VRE_costs.csv"),index_col=0)

    # Read availability data
    availability = {tech: pd.read_csv(path_to_file(sys.argv[1:],f"{tech}.csv"), index_col=0) for tech in vre_cost.index}

    print("Data loaded")

    # map = {"type":"map","rank":1,"index_type":"str","index_name":index_name,"data":{}}
    climate_years = [1995,2008,2009]
    with DatabaseMapping(url_db_out) as db_map:
        
        ## Empty the database
        db_map.purge_items('entity')
        db_map.purge_items('parameter_value')
        db_map.purge_items('alternative')
        db_map.purge_items('scenario')
        db_map.refresh_session()

        add_alternative(db_map,"Base")

        CY_index = {"iso":[],"standard":[]}
        for CY in climate_years:
            indexes = time_index(CY)
            CY_index["iso"]      += indexes[1]
            CY_index["standard"] += indexes[0]

        add_entity(db_map,"commodity",("elec",))
        add_entity(db_map,"technology_type",("wind-on",))
        add_entity(db_map,"technology_type",("wind-off",))
        add_entity(db_map,"technology_type",("solar-PV",))

        vre_cost.fillna(0.0,inplace=True)
        for tech in vre_cost.index:
            add_entity(db_map,"technology",(tech,))
            add_entity(db_map,"technology__to_commodity",(tech,"elec"))
            tech_type = "wind-on" if "wind-on" in tech else ("wind-off" if "wind-off" in tech else "solar-PV")
            add_entity(db_map,"technology_type__technology",(tech_type,tech))

            map_icost = {"type":"map","index_type":"str","index_name":"period","data":{"y"+year:round(float(vre_cost.at[tech,"capex_"+year])*1e6,1) for year in ["2030","2040","2050"] if vre_cost.at[tech,"capex_"+year] != 0.0}}
            map_fcost = {"type":"map","index_type":"str","index_name":"period","data":{"y"+year:float(vre_cost.at[tech,"fom_"+year]) for year in ["2030","2040","2050"] if vre_cost.at[tech,"fom_"+year] != 0.0}}
            map_vcost = {"type":"map","index_type":"str","index_name":"period","data":{"y"+year:float(vre_cost.at[tech,"vom_"+year]) for year in ["2030","2040","2050"] if vre_cost.at[tech,"vom_"+year] != 0.0}}
            
            if bool(map_icost["data"]):
                add_parameter_value(db_map,"technology__to_commodity","investment_cost","Base",(tech,"elec"),map_icost)
            if bool(map_fcost["data"]):
                add_parameter_value(db_map,"technology__to_commodity","fixed_cost","Base",(tech,"elec"),map_fcost)
            if bool(map_vcost["data"]):
                add_parameter_value(db_map,"technology__to_commodity","operational_cost","Base",(tech,"elec"),map_vcost)
            add_parameter_value(db_map,"technology","lifetime","Base",(tech,),float(vre_cost.at[tech,"lifetime"]))

        ## ONSHORE EXISTING
        for poly in existing_wind_on.index:
            tech = "wind-on-existing"
            if existing_wind_on.round(2).at[poly] > 0 and poly in availability[tech].columns:

                add_region(db_map, poly, "onshore", "PECD2")
                add_technology_relationship(db_map, "wind-on", tech, poly, potential_wind_on.at[poly] - (existing_wind_on.at[poly] if poly in existing_wind_on.index else 0.0), availability[tech], CY_index)

                existing = round(float(existing_wind_on.at[poly]*1e3),1)
                add_entity(db_map,"technology__region",(tech,poly))
                map_existing = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":existing}}
                add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),map_existing)                
        print("existing_wind_onshore")

        ## ONSHORE FUTURE
        technologies = ["wind-on-SP335-HH100","wind-on-SP335-HH150","wind-on-SP277-HH100","wind-on-SP277-HH150","wind-on-SP199-HH100","wind-on-SP199-HH150"]
        for tech in technologies:
            for poly in availability[tech].columns:
                if poly in potential_wind_on.index:
                    add_region(db_map, poly, "onshore", "PECD2")
                    add_technology_relationship(db_map, "wind-on", tech, poly, potential_wind_on.at[poly] - (existing_wind_on.at[poly] if poly in existing_wind_on.index else 0.0), availability[tech], CY_index)
        print("wind_on_future")

        ## ONSHORE SOLAR
        share = {"solar-PV-no-tracking":0.8,"solar-PV-rooftop":0.2,"solar-PV-tracking":0.0}
        technologies = ["solar-PV-no-tracking","solar-PV-rooftop","solar-PV-tracking"]
        for tech in technologies:
            for poly in existing_solar_PV.index:
                if poly in availability[tech].columns and poly in potential_solar_PV.index:
                    add_region(db_map, poly, "onshore", "PECD2")
                    add_technology_relationship(db_map, "solar-PV", tech, poly, potential_solar_PV.at[poly] - (existing_solar_PV.at[poly] if poly in existing_solar_PV.index else 0.0), availability[tech], CY_index)

                    existing = round(float(share[tech]*existing_solar_PV.at[poly]*1e3),1)
                    add_entity(db_map,"technology__region",(tech,poly))
                    map_existing = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":existing}}
                    add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),map_existing) 
        print("Solar-PV")

        ## OFFSHORE TECHNOLOGY
        ### OFFSHORE EXISTING
        for poly in existing_wind_off.index:
            tech = "wind-off-existing"
            if existing_wind_off.round(2).at[poly] > 0 and poly in availability[tech].columns:
                add_region(db_map, poly, "offshore", "OFF3")
                add_technology_relationship(db_map, "wind-off", tech, poly, potential_wind_off.at[poly] - (existing_wind_off.at[poly] if poly in existing_wind_off.index else 0.0), availability[tech], CY_index)

                existing = round(float(existing_wind_off.at[poly]*1e3),1)
                add_entity(db_map,"technology__region",(tech,poly))
                map_existing = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":existing}}
                add_parameter_value(db_map,"technology__region","units_existing","Base",(tech,poly),map_existing)   
        print("existing_wind_offshore")

        ### OFFSHORE FUTURE
        technologies = ["wind-off-FB-SP316-HH155","wind-off-FB-SP370-HH155"]
        for tech in technologies:
            for poly in availability[tech].columns:    
                if poly in potential_wind_off.index:
                    add_region(db_map, poly, "offshore", "OFF3")
                    add_technology_relationship(db_map, "wind-off", tech, poly, potential_wind_off.at[poly] - (existing_wind_off.at[poly] if poly in existing_wind_off.index else 0.0), availability[tech], CY_index)
        print("wind_off_future")

        db_map.commit_session("entities added")

if __name__ == "__main__":
    main()
    