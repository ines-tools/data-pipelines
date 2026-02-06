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

if len(sys.argv) > 2:
    url_result = sys.argv[1]
    url_spineopt = sys.argv[2]
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

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)
    
def add_scenario_alternative(db_map : DatabaseMapping,name_scenario : str, name_alternative : str, rank_int = None) -> None:
    _, error = db_map.add_scenario_alternative_item(scenario_name = name_scenario, alternative_name = name_alternative, rank = rank_int)
    if error is not None:
        raise RuntimeError(error)

def get_latest_alternatives():

    with DatabaseMapping(url_spineopt) as spineopt_db:
        predefined_scenarios = [scen_map["name"] for scen_map in spineopt_db.get_scenario_items()]

    with DatabaseMapping(url_result) as result_db:
        alternatives = [i["name"] for i in result_db.get_alternative_items()]
        latest_alternatives = {}
        for alternative in alternatives:
            if "@" in alternative:
                name, timestamp = alternative.split('@')
                timestamp = pd.Timestamp(timestamp)
            
                if (name not in latest_alternatives or timestamp > latest_alternatives[name]) and any(i in name for i in predefined_scenarios):
                    latest_alternatives[name] = timestamp

    return latest_alternatives

def get_invested_available(latest_alternatives):

    invested_available_items = {}
    with DatabaseMapping(url_result) as result_db:

        for parameter_name in ["units_invested_available","connections_invested_available","storages_invested_available"]:
            invested_available_items[parameter_name] = {}
            for parameter_map in result_db.get_parameter_value_items(parameter_definition_name = parameter_name):
                entity_name = parameter_map["entity_byname"][1]
                if entity_name not in invested_available_items[parameter_name]:
                    invested_available_items[parameter_name][entity_name] = {}
                
                scenario_name, timestamp = parameter_map["alternative_name"].split("@")
                timestamp = pd.Timestamp(timestamp)
                if scenario_name in latest_alternatives:
                    if timestamp == latest_alternatives[scenario_name]:

                        map_table = convert_map_to_table(parameter_map["parsed_value"])
                        index_names = nested_index_names(parameter_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + [entity_name]).set_index(index_names[0])[entity_name]
                
                        invested_available_items[parameter_name][entity_name][scenario_name] = data
   
    
    return invested_available_items

def fix_invested_available(invested_available_items):

    target_parameter = {"units_invested_available":"number_of_units","connections_invested_available":"number_of_connections","storages_invested_available":"number_of_storages"}
    target_class = {"units_invested_available":"unit","connections_invested_available":"connection","storages_invested_available":"node"}
    with DatabaseMapping(url_spineopt) as spineopt_db:
        
        for parameter_name in invested_available_items:
            for entity_name in invested_available_items[parameter_name]:
                for alternative_name in invested_available_items[parameter_name][entity_name]:
                    data = invested_available_items[parameter_name][entity_name][alternative_name]
                    for index_ in data.index:
                        new_alternative_name = "y"+str(index_.year)+"_"+alternative_name.split("__")[0]
                        try:
                            add_alternative(spineopt_db,new_alternative_name)
                        except:
                            pass
                        parameter_value = data.at[index_]
                        add_or_update_parameter_value(spineopt_db,target_class[parameter_name],target_parameter[parameter_name],new_alternative_name,(entity_name,),parameter_value)
        spineopt_db.commit_session("number_of_added")

def eliminate_investment_variables():
    to_remove_parameters = ["fix_units_invested",
                            "fix_connections_invested",
                            "fix_storages_invested",
                            "candidate_units",
                            "candidate_connections",
                            "candidate_storages",
                            "fix_units_invested_available",
                            "initial_units_invested_available",
                            "number_of_units",
                            "fix_connections_invested_available",
                            "initial_connections_invested_available",
                            "number_of_connections",
                            "fix_storages_invested_available",
                            "initial_storages_invested_available",
                            "number_of_storages"]
    with DatabaseMapping(url_spineopt) as spineopt_db:
        for parameter_name in to_remove_parameters:
            for parameter_map in spineopt_db.get_parameter_value_items(parameter_definition_name = parameter_name):
                item_id = parameter_map["id"]
                spineopt_db.remove_item("parameter_value",item_id)
        spineopt_db.commit_session("eliminate_candidates")

def eliminate_scenarios():

    with DatabaseMapping(url_spineopt) as spineopt_db:
        for scenario_map in spineopt_db.get_scenario_items():
            item_id = scenario_map["id"]
            spineopt_db.remove_item("scenario",item_id)
        try:
            spineopt_db.commit_session("eliminate_scenarios")
        except DBAPIError as e:
            print("###################################################################### commit error")  

def delete_investment_groups():
    with DatabaseMapping(url_spineopt) as spineopt_db:
        for entity_map in spineopt_db.get_entity_items(entity_class_name = "investment_group"):
            item_id = entity_map["id"]
            spineopt_db.remove_item("entity",item_id)
        spineopt_db.commit_session("eliminate_investment_groups")

def delete_unused_alternatives():
    with DatabaseMapping(url_spineopt) as spineopt_db:
        for alt_map in spineopt_db.get_alternative_items():
            alt_in_parameter = spineopt_db.get_parameter_value_items(alternative_name = alt_map["name"])
            if not alt_in_parameter:
                item_id = alt_map["id"]
                spineopt_db.remove_item("alternative",item_id)
        spineopt_db.commit_session("eliminate_unused_alternatives")

def eliminate_investment_temporal_block(model_stage=True):

    with DatabaseMapping(url_spineopt) as spineopt_db:
        model_name = [entity_i["name"] for entity_i in spineopt_db.get_entity_items(entity_class_name="model")][0]
        add_entity(spineopt_db,"temporal_block",("operations",))
        add_entity(spineopt_db,"model__default_temporal_block",(model_name,"operations"))
        add_parameter_value(spineopt_db,"temporal_block","resolution","Base",("operations",),{"type":"duration","data":"1h"})

        for cyclic_map in spineopt_db.get_parameter_value_items(entity_class_name = "node__temporal_block", parameter_definition_name = "cyclic_condition"):
            if bool(cyclic_map["parsed_value"]):
                try:
                    add_entity(spineopt_db,"node__temporal_block",(cyclic_map["entity_byname"][0],"operations"))
                    add_parameter_value(spineopt_db,"node__temporal_block","cyclic_condition","Base",(cyclic_map["entity_byname"][0],"operations"),True)
                except:
                    pass

        for entity_map in spineopt_db.get_entity_items(entity_class_name = "temporal_block"):
            if "planning" not in entity_map["name"] and "representative_period" not in entity_map["name"] and "all_rps" not in entity_map["name"]:
                for parameter_name in ["block_start","block_end"]:
                    for parameter_map in spineopt_db.get_parameter_value_items(parameter_definition_name = parameter_name, entity_byname = entity_map["entity_byname"]):
                        year = "y2030" if "2030" in parameter_map["entity_name"] else ("y2040" if "2040" in parameter_map["entity_name"] else "y2050")
                        try:
                            add_alternative(spineopt_db,year)
                        except:
                            pass
                        if parameter_name == "block_start":
                            parameter_value = json.loads(parameter_map["value"])
                            add_or_update_parameter_value(spineopt_db,"model","model_start",year,(model_name,),parameter_value)
                        elif parameter_name == "block_end":
                            parameter_value = json.loads(parameter_map["value"])
                            add_or_update_parameter_value(spineopt_db,"model","model_end",year,(model_name,),parameter_value)
            if "planning" in entity_map["name"] or "operations_" in entity_map["name"] or "representative_period" in entity_map["name"] or "all_rps" in entity_map["name"]:
                item_id = entity_map["id"]
                spineopt_db.remove_item("entity",item_id)

        spineopt_db.commit_session("eliminate_investment_temporal_block")

def update_model(model_stage):
    with DatabaseMapping(url_spineopt) as spineopt_db:
        for parameter_name in ["model_start","model_end"]:
            for parameter_map in spineopt_db.get_parameter_value_items(parameter_definition_name = parameter_name):
                item_id = parameter_map["id"]
                spineopt_db.remove_item("parameter_value",item_id)
        model_name = [entity_i["name"] for entity_i in spineopt_db.get_entity_items(entity_class_name="model")][0]
        if model_stage:
            add_parameter_value(spineopt_db,"model","roll_forward","Base",(model_name,),{"type":"duration","data":"24h"})
        
        for entity_map in spineopt_db.get_entity_items(entity_class_name = "model__default_investment_stochastic_structure"):
            item_id = entity_map["id"]
            spineopt_db.remove_item("entity",item_id)
        spineopt_db.commit_session("eliminate_candidates")

def scenario_definition(model_stage):

    with DatabaseMapping(url_spineopt) as spineopt_db:

        scenarios = {scen_map["name"]:[scen_alt["alternative_name"] for i in range(1,len(spineopt_db.get_scenario_alternative_items(scenario_name = scen_map["name"]))+1) for scen_alt in spineopt_db.get_scenario_alternative_items(scenario_name = scen_map["name"], rank = i)] for scen_map in spineopt_db.get_scenario_items()}
        years = ["y2030","y2040","y2050"]
        eliminate_scenarios()

        if model_stage:
            stage_name = "lt_storage"
            stage_alternative = "lt_storage_alt"
            add_entity(spineopt_db,"stage", (stage_name,))
            add_alternative(spineopt_db,stage_alternative)
            add_parameter_value(spineopt_db,"temporal_block","resolution",stage_alternative,("operations",),{"type":"duration","data":"8h"})
            model_name = [entity_i["name"] for entity_i in spineopt_db.get_entity_items(entity_class_name="model")][0]
            add_parameter_value(spineopt_db,"model","roll_forward",stage_alternative,(model_name,),None)
            
            for has_state in spineopt_db.get_parameter_value_items(entity_class_name = "node", parameter_definition_name = "has_state"):
                if bool(has_state["parsed_value"]):
                    for output in ["node_state"]:
                        add_entity(spineopt_db,"stage__output__node",(stage_name,output,has_state["entity_byname"][0]))
            
            for cyclic_map in spineopt_db.get_parameter_value_items(entity_class_name = "node__temporal_block", parameter_definition_name = "cyclic_condition"):
                if bool(cyclic_map["parsed_value"]):
                    add_parameter_value(spineopt_db,"node__temporal_block","cyclic_condition",stage_alternative,(cyclic_map["entity_byname"][0],"operations"),True)
                    add_or_update_parameter_value(spineopt_db,"node__temporal_block","cyclic_condition","Base",(cyclic_map["entity_byname"][0],"operations"),False)

        for scenario,alternatives in scenarios.items():
            for year in years:
                new_scenario = year+"_"+scenario
                add_scenario(spineopt_db,new_scenario)
                for alternative in alternatives:
                    add_scenario_alternative(spineopt_db,new_scenario,alternative,alternatives.index(alternative)+1)

                add_scenario_alternative(spineopt_db,new_scenario,year,len(alternatives)+1)
                add_scenario_alternative(spineopt_db,new_scenario,new_scenario,len(alternatives)+2)

                if model_stage:
                    stage_scenario = f"lt_storage_{year}_{scenario}"
                    add_scenario(spineopt_db,stage_scenario)
                    add_parameter_value(spineopt_db,"stage","stage_scenario",new_scenario,(stage_name,),stage_scenario)

                    for alternative in alternatives:
                        add_scenario_alternative(spineopt_db,stage_scenario,alternative,alternatives.index(alternative)+1)

                    add_scenario_alternative(spineopt_db,stage_scenario,year,len(alternatives)+1)
                    add_scenario_alternative(spineopt_db,stage_scenario,new_scenario,len(alternatives)+2)
                    add_scenario_alternative(spineopt_db,stage_scenario,stage_alternative,len(alternatives)+3)

                    
        spineopt_db.commit_session("Added scenario")

def add_slack_var_demand():
    with DatabaseMapping(url_spineopt) as spineopt_db:
        for parameter_name in ["demand","fractional_demand"]:
            for parameter_map in spineopt_db.get_parameter_value_items(parameter_definition_name = parameter_name):
                entity_name = parameter_map["entity_name"]
                if parameter_map["type"] == "time_series":
                    map_table = convert_map_to_table(parameter_map["parsed_value"])
                    index_names = nested_index_names(parameter_map["parsed_value"])
                    data = pd.DataFrame(map_table, columns=index_names + [entity_name]).set_index(index_names[0])[entity_name]
                    data_positive = True if data.mean() > 0.0 else False
                else:
                    data = parameter_map["parsed_value"]
                    data_positive = True if data > 0.0 else False
                if data_positive:
                    try:
                        add_parameter_value(spineopt_db,"node","node_slack_penalty","Base",(entity_name,),1e4)
                    except:
                        pass
        spineopt_db.commit_session("Added slack variable")

def main():
    
    latest_alternatives = get_latest_alternatives()
    invested_available_items = get_invested_available(latest_alternatives)
    eliminate_investment_variables()
    fix_invested_available(invested_available_items)
    add_slack_var_demand()
    delete_investment_groups()
    model_stage = True
    update_model(model_stage)
    eliminate_investment_temporal_block()
    scenario_definition(model_stage)
    delete_unused_alternatives()


if __name__ == "__main__":
    main()