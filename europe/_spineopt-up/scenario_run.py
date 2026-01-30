import spinedb_api as api
from spinedb_api import DatabaseMapping
from spinedb_api.dataframes import to_dataframe
from spinedb_api.parameter_value import convert_map_to_table, IndexedValue
from sqlalchemy.exc import DBAPIError
import datetime
import pandas as pd
import sys
import numpy as np
import json
import yaml 
import time as time_lib

if len(sys.argv) > 1:
    url_spineopt = sys.argv[1]
else:
    exit("Please provide spineopt database url as argument. They should be of the form ""sqlite:///path/db_file.sqlite""")

def nested_index_names(value, names = None, depth = 0):
    if names is None:
        names = []
    if depth == len(names):
        names.append(value.index_name)
    elif value.index_name != names[-1]:
        raise RuntimeError(f"Index names at depth {depth} do no match: {value.index_name} vs. {names[-1]}")
    for y in value.values:
        if isinstance(y, IndexedValue):
            nested_index_names(y, names, depth + 1)
    return names

def update_parameter_value(db_map : DatabaseMapping, id_int : int, class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.update_parameter_value_item(id=id_int, entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_or_update_parameter_value(db_map : DatabaseMapping, class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    db_map.add_or_update_parameter_value(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)

def add_entity_group(db_map : DatabaseMapping, class_name : str, group : str, member : str) -> None:
    _, error = db_map.add_entity_group_item(group_name = group, member_name = member, entity_class_name=class_name)
    if error is not None:
        raise RuntimeError(error)

def add_entity(db_map : DatabaseMapping, class_name : str, name : tuple, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)

def add_scenario_alternative(db_map : DatabaseMapping,name_scenario : str, name_alternative : str, rank_int = None) -> None:
    _, error = db_map.add_scenario_alternative_item(scenario_name = name_scenario, alternative_name = name_alternative, rank = rank_int)
    if error is not None:
        raise RuntimeError(error)

def scenario_development(config):

    with DatabaseMapping(url_spineopt) as sopt_db:

        for scenario_name in config["scenarios"]:
            add_scenario(sopt_db,scenario_name)
            alt_names = config["scenarios"][scenario_name]
            for alt_name in alt_names:
                add_scenario_alternative(sopt_db,scenario_name,alt_name,alt_names.index(alt_name)+1)
        try:
            sopt_db.commit_session("Added scenario")
        except DBAPIError as e:
            print("###################################################################### commit error")  

def update_parameters(config):

    with DatabaseMapping(url_spineopt) as sopt_db:

        resolution_ = config["resolution"]

        add_or_update_parameter_value(sopt_db, "temporal_block", "resolution", "Base", ("operations_y2030", ),  {"type":"duration","data":resolution_})
        add_or_update_parameter_value(sopt_db, "temporal_block", "resolution", "Base", ("operations_y2040", ),  {"type":"duration","data":resolution_})
        add_or_update_parameter_value(sopt_db, "temporal_block", "resolution", "Base", ("operations_y2050", ),  {"type":"duration","data":resolution_})
        add_or_update_parameter_value(sopt_db, "node", "initial_storages_invested_available", "Base", ("CO2", ), 0.2*1e6/config["emission_factor"])
        add_or_update_parameter_value(sopt_db, "node", "fix_storages_invested_available", "Base", ("CO2", ), 0.2*1e6/config["emission_factor"])
        add_or_update_parameter_value(sopt_db, "node", "node_state_cap", "Base", ("atmosphere", ), 2.2*1e9/config["emission_factor"])

        try:
            sopt_db.commit_session("Update parameters")
        except DBAPIError as e:
            print("###################################################################### commit error")  

def main():

    with open(sys.argv[2], 'r') as file:
        config = yaml.safe_load(file)

    print("adding scenarios to be analyzed")
    scenario_development(config)

    print("updating_parameters")
    update_parameters(config)

if __name__ == "__main__":
    main()