import spinedb_api as api
from spinedb_api import DatabaseMapping
from sqlalchemy.exc import DBAPIError
import datetime
import pandas as pd
import sys
from openpyxl import load_workbook
import numpy as np
import json
import yaml 
import time as time_lib

def add_superclass_subclass(db_map : DatabaseMapping, superclass_name : str, subclass_name : str) -> None:
    _, error = db_map.add_superclass_subclass_item(superclass_name=superclass_name, subclass_name=subclass_name)
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

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)

def define_polygons(config : dict, region_data : dict) -> dict:
    
    countries = [
        "AT",  # Austria
        "BE",  # Belgium
        "BG",  # Bulgaria
        "HR",  # Croatia
        "CY",  # Cyprus
        "CZ",  # Czech Republic
        "DK",  # Denmark
        "EE",  # Estonia
        "FI",  # Finland
        "FR",  # France
        "DE",  # Germany
        "GR",  # Greece
        "HU",  # Hungary
        "IE",  # Ireland
        "IT",  # Italy
        "LV",  # Latvia
        "LT",  # Lithuania
        "LU",  # Luxembourg
        "MT",  # Malta
        "NL",  # Netherlands
        "PL",  # Poland
        "PT",  # Portugal
        "RO",  # Romania
        "SK",  # Slovakia
        "SI",  # Slovenia
        "ES",  # Spain
        "SE",  # Sweden
        "CH",  # Switzerland
        "UK",  # United Kingdom
        "NO"   # Norway
    ]
    polygons={"onshore":{},"offshore":{}}
    for country_id in countries:
        if country_id not in config["countries"] and "Europe" in config["countries"]:
            on_level  = config["countries"]["Europe"]["onshore"]
            off_level = config["countries"]["Europe"]["offshore"]
            on_poly   = region_data[on_level][region_data[on_level].country == country_id].id.tolist()
            off_poly  = region_data[off_level][region_data[off_level].country == country_id].id.tolist()
            polygons["onshore"].update(dict(zip(on_poly,[on_level]*len(on_poly))))
            polygons["offshore"].update({item_p:[off_level,region_data[off_level+"_map"][region_data[off_level+"_map"].source==item_p][on_level].tolist()[0]] for item_p in off_poly})
        elif country_id in config["countries"]:
            on_level  = config["countries"][country_id]["onshore"]
            off_level = config["countries"][country_id]["offshore"]
            on_poly   = region_data[on_level][region_data[on_level].country == country_id].id.tolist()
            off_poly  = region_data[off_level][region_data[off_level].country == country_id].id.tolist()
            polygons["onshore"].update(dict(zip(on_poly,[on_level]*len(on_poly))))
            polygons["offshore"].update({item_p:[off_level,region_data[off_level+"_map"][region_data[off_level+"_map"].source==item_p][on_level].tolist()[0]] for item_p in off_poly})
    return polygons

def user_entity_condition(config,entity_class_elements,entity_names,poly,poly_type):

    if poly_type == "off":
        poly_level,poly_connection = config[f"{poly_type}shore_polygons"][poly]
    else:
        poly_level = config[f"{poly_type}shore_polygons"][poly]

    entity_target_names = []
    definition_condition = True
    # Processing entity to get target names and statuses
    for index,element in enumerate(entity_class_elements):
        entity_dict = config["user"].get(element,{}).get(entity_names[index],{})
        status = config["user"][element][entity_names[index]]["status"] if entity_dict else True
        entity_new_name = entity_names[index]+status*("_"+(poly_connection if poly_type == "off" and element == "commodity" else poly))
        entity_target_names.append(entity_new_name)
        if element != "commodity":
            definition_condition *= status

    return entity_target_names,definition_condition,poly_level

def ines_aggregrate(db_source : DatabaseMapping,transformer_df : pd.DataFrame,target_poly : str ,entity_class : tuple,entity_names : tuple,source_parameter : str,weight : str,defaults = None) -> dict:

    # db_source : Spine DB
    # transformer : dataframes
    # target/source_poly : spatial resolution name
    # weight : conversion factor 
    # defaults : default value implemented

    values_ = {}
    for source_poly in transformer_df.loc[transformer_df.target == target_poly,"source"].tolist():
        
        entity_bynames = entity_names+(source_poly,)
        multiplier = transformer_df.loc[transformer_df.source == source_poly,weight].tolist()[0]
        parameter_values = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_bynames,parameter_definition_name=source_parameter)
        
        if parameter_values:
            for parameter_value in parameter_values:
                if parameter_value["type"] == "time_series":
                    param_value = json.loads(parameter_value["value"].decode("utf-8"))["data"]
                    keys = list(param_value.keys())
                    vals = multiplier*np.fromiter(param_value.values(), dtype=float)
                    if not values_.get(parameter_value["alternative_name"],{}):
                        values_[parameter_value["alternative_name"]]  = {"type":"time_series","data":dict(zip(keys,vals))}
                    else:
                        prev_vals = np.fromiter(values_[parameter_value["alternative_name"]] ["data"].values(), dtype=float)
                        values_[parameter_value["alternative_name"]]  = {"type":"time_series","data":dict(zip(keys,prev_vals + vals))}  
                elif parameter_value["type"] == "map":
                    param_dict = json.loads(parameter_value["value"].decode("utf-8"))
                    if "type" not in param_dict["data"]:
                        param_value = param_dict["data"]
                        keys = list(param_value.keys())
                        vals = multiplier*np.fromiter(param_value.values(), dtype=float)
                        if not values_.get(parameter_value["alternative_name"],{}):
                            values_[parameter_value["alternative_name"]] = {"type":"map","index_type":param_dict["index_type"],"index_name":param_dict["index_name"],"data":dict(zip(keys,vals))}
                        else:
                            prev_vals = np.fromiter(values_[parameter_value["alternative_name"]]["data"].values(), dtype=float)
                            values_[parameter_value["alternative_name"]] = {"type":"map","index_type":param_dict["index_type"],"index_name":param_dict["index_name"],"data":dict(zip(keys,prev_vals + vals))}
                elif parameter_value["type"] == "float":
                    values_[parameter_value["alternative_name"]] = values_[parameter_value["alternative_name"]] + multiplier*parameter_value["parsed_value"] if values_.get(parameter_value["alternative_name"],{}) else multiplier*parameter_value["parsed_value"]
                # ADD MORE Parameter Types HERE            
    return values_
        
def spatial_transformation(db_source, config, sector):
    
    spatial_data = {}
    for entity_class in config["sys"][sector]["entities"]:
        entity_class_region = f"{entity_class}__region"
        dynamic_params = config["sys"][sector]["parameters"]["dynamic"].get(entity_class_region, {})
        
        if dynamic_params:
            spatial_data[entity_class] = {}
            for entity_class_target, param_source_dict in dynamic_params.items():
                for source_parameter in param_source_dict:
                    spatial_data[entity_class][source_parameter] = {}
                    entities = db_source.get_entity_items(entity_class_name = entity_class)
                    for entity in entities:
                        entity_name = entity["name"]
                        entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
                        entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
                        
                        spatial_data[entity_class][source_parameter][entity_name] = {}
                        poly_type = "off" if "wind-off" in entity_name else "on"

                        param_list_target = dynamic_params[entity_class_target][source_parameter]  
                        defaults = param_list_target[3]
                        source_level = param_list_target[4][poly_type] if isinstance(param_list_target[4],dict) else param_list_target[4]     
                        multipliers = param_list_target[2]
                        if not multipliers[1]:
                            weight = multipliers[0] 
                        else:
                            for particular_case in multipliers[1]:
                                weight = multipliers[1][particular_case] if any(particular_case in entity_item for entity_item in entity_names) else multipliers[0]
                                break

                        for target_poly in config[f"{poly_type}shore_polygons"]:
                            _,definition_condition,target_level = user_entity_condition(config,entity_class_elements,entity_names,target_poly,poly_type)

                            if definition_condition == True:
                                
                                if source_level != target_level:  
                                    
                                    value_aggregate = ines_aggregrate(db_source,config["transformer"][f"{source_level}_{target_level}"],target_poly,entity_class_region,entity_names,source_parameter,weight,defaults)
                                    if isinstance(defaults,float) and not value_aggregate:
                                        # Only default values for existing capacity
                                        value_aggregate = {"Base":{"type":"map","index_type":"str","index_name":"period","data":{"y2030":defaults}}}
                                    spatial_data[entity_class][source_parameter][entity_name][target_poly] = value_aggregate
                                else:
                                    entity_bynames = entity_names+(target_poly,)
                                    parameter_values = db_source.get_parameter_value_items(entity_class_name=entity_class_region,entity_byname=entity_bynames,parameter_definition_name=source_parameter)
                                    if parameter_values:
                                        spatial_data[entity_class][source_parameter][entity_name][target_poly] = {}
                                        for parameter_value in parameter_values:
                                            if parameter_value["type"] == "time_series":
                                                param_value = json.loads(parameter_value["value"].decode("utf-8"))["data"]
                                                keys = list(param_value.keys())
                                                vals = np.fromiter(param_value.values(), dtype=float)
                                                value_ = {"type":"time_series","data":dict(zip(keys,vals))}     
                                            elif parameter_value["type"] == "map":
                                                param_dict = json.loads(parameter_value["value"].decode("utf-8"))
                                                if "type" not in param_dict["data"]:
                                                    param_value = param_dict["data"]
                                                    keys = list(param_value.keys())
                                                    vals = np.fromiter(param_value.values(), dtype=float)
                                                    value_ = {"type":"map","index_type":param_dict["index_type"],"index_name":param_dict["index_name"],"data":dict(zip(keys,vals))}     
                                            elif parameter_value["type"] == "float":
                                                value_ = parameter_value["parsed_value"]
                                            spatial_data[entity_class][source_parameter][entity_name][target_poly][parameter_value["alternative_name"]] = value_
                                    elif defaults != None:
                                        # Only default values for existing capacity
                                        value_ = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":defaults}}
                                        spatial_data[entity_class][source_parameter][entity_name][target_poly] = {"Base":value_}
                                    else:
                                        spatial_data[entity_class][source_parameter][entity_name][target_poly] = {} 
    return spatial_data

def add_timeline(db_map : DatabaseMapping,config : dict):

    period_dict = {"type": "array","value_type": "str","data": []}
    for year in config["user"]["model"]["planning_years"]:
        add_entity(db_map, "period", ("y"+year,))
        add_parameter_value(db_map,"period","years_represented","Base",("y"+year,),config["user"]["model"]["planning_years"][year][1])
        add_parameter_value(db_map,"period","start_time","Base",("y"+year,),{"type":"date_time","data":config["user"]["model"]["planning_years"][year][0]})
        period_dict["data"].append("y"+year)

    # temporality
    wy_dict = {"type": "array","value_type": "date_time","data": [config["user"]["timeline"]["historical_alt"][i]["start"] for i in config["user"]["timeline"]["historical_alt"]]}
    add_entity(db_map, "solve_pattern", ("capacity_planning",))
    add_parameter_value(db_map,"solve_pattern","time_resolution","Base",("capacity_planning",),{"type":"duration","data":config["user"]["model"]["operations_resolution"]})
    add_parameter_value(db_map,"solve_pattern","duration","Base",("capacity_planning",),{"type":"duration","data":config["user"]["model"]["planning_resolution"]})
    add_parameter_value(db_map,"solve_pattern","period","Base",("capacity_planning",),period_dict)
    add_parameter_value(db_map,"solve_pattern","start_time","Base",("capacity_planning",),wy_dict)

def add_nodes(db_map : DatabaseMapping, db_com : DatabaseMapping, config : dict) -> None:

    entity_class = "node"
    entity_nodes = [entity_i["name"] for entity_i in db_map.get_entity_items(entity_class_name = entity_class) if not db_map.get_parameter_value_item(entity_class_name="node",entity_byname=(entity_i["name"],),parameter_definition_name="node_type",alternative_name="Base")]
    for entity_node in entity_nodes:
        list_names = entity_node.split("_")
        if list_names[0] in config["user"]["commodity"]:
            add_parameter_value(db_map,"node","node_type","Base",(entity_node,),config["user"]["commodity"][list_names[0]]["node_type"])
            if config["user"]["commodity"][list_names[0]]["node_type"] == "commodity":
                param_list = config["sys"]["commodities"]["commodity"][entity_class]
                for param_source in param_list:
                    param_target = param_list[param_source][0]
                    multiplier = param_list[param_source][1]
                    values_ = db_com.get_parameter_value_items(entity_class_name="commodity",entity_byname=(list_names[0],),parameter_definition_name=param_source)
                    if values_:
                        for value_ in values_:
                            value_param = multiplier*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:multiplier*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                            add_parameter_value(db_map,entity_class,param_target,value_["alternative_name"],(entity_node,),value_param)
        else:
            add_parameter_value(db_map,"node","node_type","Base",(entity_node,),"balance")

def add_electricity_demand(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:
    db_name = "elec_demand"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING ELEC DEMAND TIME SERIES")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")

                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        
                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])

                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                for alternative in region_params[entity_class][param_source][entity_name][poly]:
                                    add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])
                                            
def add_power_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "power_sector"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING POWER ELEMENTS")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
                # checking hard-coding conditions
                if "technology" in entity_class_elements and definition_condition == True:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="technology"]:
                        existing_dict = region_params.get("technology",{}).get("units_existing",{}).get(entity_names[index_in_class],{})
                        if existing_dict and config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            if sum(sum(existing_dict[poly][alternative]["data"].values()) for alternative in existing_dict[poly]) == 0.0:
                                definition_condition *= False
                        elif config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            definition_condition *= False

                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                            for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                                try:
                                    add_entity(db_map,entity_class_target,entity_target_name)
                                except RuntimeError:
                                    print(f"Repeated Entity {entity_class} {entity_name}, then not added")
                                    pass
      
                        # User Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["user"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["user"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["user"][entity_class][entity_class_target]
                                for param_target in param_list:
                                    entity_source_name = "__".join([entity_names[i-1] for k in param_list[param_target][2] for i in k])
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_target][3]])
                                    add_parameter_value(db_map,entity_class_target,param_target,"Base",entity_target_name,config["user"][param_list[param_target][0]][entity_source_name][param_list[param_target][1]])

                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        
                        # Fixed Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                    if values_:
                                        for value_ in values_:
                                            value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                            add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)
                        
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                for alternative in region_params[entity_class][param_source][entity_name].get(poly,{}):
                                    add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])
                            
def add_vre_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, "vre")
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")
    print("ADDING VRE ELEMENTS")
    for entity_class in config["sys"]["vre"]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        # print(f"{entity_class} turn")
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            
            poly_type = "off" if "wind-off" in entity_name else "on"
            for poly in config[f"{poly_type}shore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,poly_type)

                # checking hard-coding conditions
                if "technology" in entity_class_elements and definition_condition == True:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="technology"]:
                        entity_name_for_potential = [i["entity_byname"][0] for i in db_source.get_entity_items(entity_class_name = "technology_type__technology") if entity_names[index_in_class] in i["entity_byname"]][0]
                        potential_dict= region_params.get("technology_type",{}).get("potential",{}).get(entity_name_for_potential,{}).get(poly,{})
                        existing_dict = region_params.get("technology",{}).get("units_existing",{}).get(entity_names[index_in_class],{})
                        if potential_dict:
                            if existing_dict and config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                                if sum(sum(existing_dict[poly][alternative]["data"].values()) for alternative in existing_dict[poly]) == 0.0:
                                    definition_condition *= False
                            elif config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                                definition_condition *= False
                        else:
                            definition_condition *= False
                    if not region_params["technology__to_commodity"]["profile_limit_upper"][entity_names[entity_class_elements.index("technology")]+"__elec"][poly]:
                        definition_condition *= False
                
                if "technology_type" in entity_class_elements:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="technology_type"]:
                        potential_dict = region_params.get("technology_type",{}).get("potential",{}).get(entity_names[index_in_class],{}).get(poly,{})
                        if not potential_dict:
                            definition_condition *=False
                        
                # print(entity_name, definition_condition)
                if definition_condition == True:
                    for entity_class_target in config["sys"]["vre"]["entities"][entity_class]:
                        # Entity Definitions
                        for entity_target_building in config["sys"]["vre"]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)
                        
                        # User Parameters
                        if entity_class in config["sys"]["vre"]["parameters"]["user"]:
                            user_params = config["sys"]["vre"]["parameters"]["user"][entity_class].get(entity_class_target, {})
                            for param_target, param_values in user_params.items():
                                entity_source_name = "__".join([entity_names[i-1] for k in param_values[2] for i in k])
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[3]])
                                add_parameter_value(db_map,entity_class_target,param_target,"Base",entity_target_name,config["user"][param_values[0]][entity_source_name][param_values[1]])
  
                        # Default Parameters
                        if entity_class in config["sys"]["vre"]["parameters"]["default"]:
                            if entity_class_target in config["sys"]["vre"]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"]["vre"]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        
                        # Fixed Parameters
                        if entity_class in config["sys"]["vre"]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"]["vre"]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"]["vre"]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                    if values_:
                                        for value_ in values_:
                                            value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                            add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param) 
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"]["vre"]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"]["vre"]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                for alternative in region_params[entity_class][param_source][entity_name].get(poly,{}):
                                    add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])

def add_hydro(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:
    
    db_name = "hydro_systems"
    print(db_name,"WARNING: Source DB must be in the user-defined target resolution")
    print("ADDING HYDRO_SYSTEMS")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []

            # entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
            if entity_names[-1] in config["onshore_polygons"]:
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_items[2]])
                                add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source,alternative_name="Base")
                                if values_:
                                    for value_ in values_:
                                        if value_["type"] == "map": 
                                            param_map = json.loads(value_["value"])
                                            value_param = {"type":"map","index_type":param_map["index_type"],"index_name":param_map["index_name"],"data":{key:param_list[param_source][1]*item for key,item in dict(param_map["data"]).items()}}
                                        elif value_["type"] == "time_series":
                                            param_map = json.loads(value_["value"].decode("utf-8"))["data"]
                                            keys = list(param_map.keys())
                                            vals = param_list[param_source][1]*np.fromiter(param_map.values(), dtype=float)
                                            value_param = {"type":"time_series","data":dict(zip(keys,vals))}
                                        else:
                                            value_param = param_list[param_source][1]*value_["parsed_value"] 
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)               

def add_power_transmission(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "power_transmission"
    print(db_name,"WARNING: Source DB must be in the user-defined target resolution")
    print("ADDING POWER TRANSMISSION")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]

            if entity_names[0] in config["onshore_polygons"] and entity_names[-1] in config["onshore_polygons"] and config["user"]["transmission"][entity_names[2]]["status"] and config["user"][entity_class_elements[2]][entity_names[2]]["status"] and config["user"][entity_class_elements[2]][entity_names[2]]["node_type"] == "balance":               
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_items[2]])
                                if param_items[0] == "investment_method": # Particular Case Screening Out
                                    original_parameter = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name="links_potentials",alternative_name="Base")
                                    value_default = "not_allowed" if not original_parameter else param_items[1]
                                else:
                                    value_default = param_items[1]
                                add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,value_default)
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                if values_:
                                    for value_ in values_:
                                        value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)
                        
def add_industrial_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config :dict) -> None:

    db_name = "industrial_sector"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING INDUSTRIAL ROUTES")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
                
                # Condition of demand at technology node
                if "technology" in entity_class_elements and definition_condition:
                    technology_name = entity_names[entity_class_elements.index("technology")]
                    technology_node = [entity_i["element_name_list"][1] for entity_i in db_source.get_entity_items(entity_class_name = "technology__to_commodity") if entity_i["element_name_list"][0] == technology_name and entity_i["element_name_list"][1] != "CO2"][0]
                    if not region_params["commodity"]["demand"][technology_node][poly]:
                        print(technology_name, f"cannot supply {technology_node} in", poly, "as demand does not exist")
                        definition_condition = False 
                    elif sum(region_params["commodity"]["demand"][technology_node][poly][alternative] for alternative in region_params["commodity"]["demand"][technology_node][poly]) == 0:
                        print(technology_name, f"cannot supply {technology_node} in", poly, "as demand equals to zero")
                        definition_condition = False 
                    
                    if definition_condition == False:
                        technology_connected = [entity_i["element_name_list"][1] for entity_i in db_source.get_entity_items(entity_class_name = "commodity__to_technology") if entity_i["element_name_list"][0] == technology_node]
                        if technology_connected and config["user"]["commodity"][technology_node]["node_type"] == "balance":
                            definition_condition = True

                # print(entity_target_names,definition_condition)
                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                            for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                                try:
                                    add_entity(db_map,entity_class_target,entity_target_name)
                                except RuntimeError:
                                    print(f"Repeated Entity {entity_class} {entity_name}, then not added")
                                    pass
      
                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        
                        # Fixed Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                    if values_:
                                        for value_ in values_:
                                            value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                            add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)
                        
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                if region_params[entity_class][param_source][entity_name].get(poly,{}):
                                    for alternative in region_params[entity_class][param_source][entity_name].get(poly,{}):
                                        add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])
                                    # Default value when demand is defined
                                    if param_source == "demand":
                                        add_parameter_value(db_map,entity_class_target,"flow_scaling_method","Base",entity_target_name,"use_profile_directly")

def add_biomass_production(db_map : DatabaseMapping, db_source : DatabaseMapping, config :dict) -> None:

    for alternative_i in db_source.get_alternative_items():
        try:
            db_map.add_alternative_item(name=alternative_i["name"])
        except RuntimeError:
            print(f"Repeated Alternative {alternative_i['name']}, then not added")
            pass

    db_name = "biomass_production"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING BIOMASS INFORMATION")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                            for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                                try:
                                    add_entity(db_map,entity_class_target,entity_target_name)
                                except RuntimeError:
                                    print(f"Repeated Entity {entity_class} {entity_name}, then not added")
                                    pass
                                
                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                for alternative in region_params[entity_class][param_source][entity_name][poly]:
                                    add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])

def add_gas_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "gas_sector"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING GAS ELEMENTS")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
                # checking hard-coding conditions
                if "technology" in entity_class_elements and definition_condition == True:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="technology"]:
                        entity_name_for_capacity = [i for i in db_source.get_entity_items(entity_class_name = "technology__to_commodity") if entity_names[index_in_class] in i["entity_byname"]][0]["name"]
                        existing_dict = region_params.get("technology",{}).get("units_existing",{}).get(entity_names[index_in_class],{})
                        if existing_dict and config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            if sum(sum(existing_dict[poly][alternative]["data"].values()) for alternative in existing_dict[poly]) == 0.0:
                                definition_condition *= False
                        elif config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            definition_condition *= False
                        
                if "storage" in entity_class_elements and definition_condition == True:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="storage"]:
                        existing_dict = region_params["storage"]["storages_existing"].get(entity_names[index_in_class],{})
                        if existing_dict and config["user"]["storage"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            if sum(sum(existing_dict[poly][alternative]["data"].values()) for alternative in existing_dict[poly]) == 0.0:
                                definition_condition *= False
                        elif config["user"]["storage"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            definition_condition *= False

                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                            for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                                try:
                                    add_entity(db_map,entity_class_target,entity_target_name)
                                except RuntimeError:
                                    print(f"Repeated Entity {entity_class} {entity_name}, then not added")
                                    pass
      
                        # User Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["user"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["user"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["user"][entity_class][entity_class_target]
                                for param_target_name,param_targets in param_list.items():
                                    for param_target in param_targets:
                                        entity_source_name = "__".join([entity_names[i-1] for k in param_target[2] for i in k])
                                        entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_target[3]])
                                        param_target_value = config["user"][param_target[0]][entity_source_name][param_target[1]]
                                        add_parameter_value(db_map,entity_class_target,param_target_name,"Base",entity_target_name,param_target_value)
        
                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        
                        # Fixed Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                    if values_:
                                        for value_ in values_:
                                            if value_["type"] != "str":
                                                value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                            else:
                                                value_param = value_["parsed_value"]
                                            add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)
                                            if param_list[param_source][0] == "storage_state_fix":
                                                add_parameter_value(db_map,entity_class_target,"storage_state_fix_method",value_["alternative_name"],entity_target_name,"fix_start")
                                            
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                for alternative in region_params[entity_class][param_source][entity_name].get(poly,{}):
                                    add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])

def add_gas_pipelines(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "gas_pipelines"
    print(db_name,"WARNING: Source DB must be in the user-defined target resolution")
    print("ADDING GAS PIPELINES")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]

            if entity_names[0] in config["onshore_polygons"] and entity_names[-1] in config["onshore_polygons"] and config["user"]["transmission"][entity_names[1]]["status"] and config["user"][entity_class_elements[1]][entity_names[1]]["node_type"] == "balance" and config["user"][entity_class_elements[1]][entity_names[1]]["status"]:               
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_items[2]])
                                if param_items[0] == "investment_method": # Particular Case Screening Out
                                    original_parameter = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name="potentials",alternative_name="Base")
                                    value_default = "not_allowed" if not original_parameter else param_items[1]
                                else:
                                    value_default = param_items[1]
                                add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,value_default)
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                if values_:
                                    for value_ in values_:
                                        if value_["type"] != "str":
                                            value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                        else:
                                            value_param = value_["parsed_value"]
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)                       

def add_transport(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    for alternative_i in db_source.get_alternative_items():
        try:
            db_map.add_alternative_item(name=alternative_i["name"])
        except RuntimeError:
            print(f"Repeated Alternative {alternative_i['name']}, then not added")
            pass

    db_name = "transport_sector"
    print(db_name,"WARNING: Source DB must be in the user-defined target resolution")
    print("ADDING TRANSPORT")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]

            if entity_names[2] in config["onshore_polygons"] and config["user"]["vehicle"][entity_names[1]]["status"] == True and config["user"]["commodity"][entity_names[0]]["node_type"] == "balance":             
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_items in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_building, dict_condition = entity_target_items
                            entity_target_name = tuple(["_".join(([entity_names[0]] if (len(k) == 2 and k[0]==1 and config["user"]["commodity"][entity_names[0]]["status"] == False) else [entity_names[i-1] for i in k])) for k in entity_target_building])
                            if not dict_condition:
                                try:
                                    add_entity(db_map,entity_class_target,entity_target_name)
                                except RuntimeError:
                                    print(f"Repeated Entity {entity_class} {entity_names} in , then not added")
                                    pass
                            else:
                                for dict_parameter  in dict_condition:
                                    value_condition = db_source.get_parameter_value_item(entity_class_name = entity["entity_class_name"], parameter_definition_name = dict_parameter, entity_byname = entity_names, alternative_name = "Base")
                                    if value_condition["parsed_value"] == dict_condition[dict_parameter]:
                                        try:
                                            add_entity(db_map,entity_class_target,entity_target_name)
                                        except RuntimeError:
                                            print(f"Repeated Entity {entity_class} {entity_name}, then not added")
                                            pass

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join(([entity_names[0]] if (len(k) == 2 and k[0]==1 and config["user"]["commodity"][entity_names[0]]["status"] == False) else [entity_names[i-1] for i in k])) for k in param_items[2]])
                                if not param_items[3]:
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                                else:
                                    for dict_parameter  in param_items[3]:
                                        value_condition = db_source.get_parameter_value_item(entity_class_name = entity["entity_class_name"], parameter_definition_name = dict_parameter, entity_byname = entity_names, alternative_name = "Base")
                                        if value_condition["parsed_value"] == param_items[3][dict_parameter]:
                                            add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join(([entity_names[0]] if (len(k) == 2 and k[0]==1 and config["user"]["commodity"][entity_names[0]]["status"] == False) else [entity_names[i-1] for i in k])) for k in param_list[param_source][2]])
                                values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                if values_:
                                    for value_ in values_:
                                        if value_["type"] == "map":
                                            param_map = json.loads(value_["value"].decode("utf-8"))
                                            param_value = param_map["data"]
                                            keys_ = list(param_value.keys())
                                            vals_ = param_list[param_source][1]*np.fromiter(param_value.values(), dtype=float)
                                            value_param =  {"type":"map","index_type":param_map["index_type"],"index_name":param_map["index_name"],"data": dict(zip(keys_,vals_))}
                                        elif value_["type"] == "float":
                                            value_param = param_list[param_source][1]*value_["parsed_value"]
                                        elif value_["type"] == "str":
                                            value_param = value_["parsed_value"]
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)  
                                                                                                        
def add_heat_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    for alternative_i in db_source.get_alternative_items():
        try:
            db_map.add_alternative_item(name=alternative_i["name"])
        except RuntimeError:
            print(f"Repeated Alternative {alternative_i['name']}, then not added")
            pass

    db_name = "heat_sector"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING HEAT ELEMENTS")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
                # checking hard-coding conditions
                if "technology" in entity_class_elements and definition_condition == True:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="technology"]:
                        if sum(sum(region_params["technology"]["units_existing"][entity_names[index_in_class]][poly][alternative]["data"].values()) for alternative in region_params["technology"]["units_existing"][entity_names[index_in_class]][poly]) == 0.0 and config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            definition_condition *= False

                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                            for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                                try:
                                    add_entity(db_map,entity_class_target,entity_target_name)
                                except RuntimeError:
                                    print(f"Repeated Entity {entity_class} {entity_name}, then not added")
                                    pass
      
                        # User Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["user"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["user"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["user"][entity_class][entity_class_target]
                                for param_target in param_list:
                                    entity_source_name = "__".join([entity_names[i-1] for k in param_list[param_target][2] for i in k])
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_target][3]])
                                    add_parameter_value(db_map,entity_class_target,param_target,"Base",entity_target_name,config["user"][param_list[param_target][0]][entity_source_name][param_list[param_target][1]])

                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        
                        # Fixed Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                    if values_:
                                        for value_ in values_:
                                            value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                            add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)
                        
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                for alternative in region_params[entity_class][param_source][entity_name].get(poly,{}):
                                    add_parameter_value(db_map,entity_class_target,param_values[0],alternative,entity_target_name,region_params[entity_class][param_source][entity_name][poly][alternative])

def add_cargo_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "cargo_transport"
    print(db_name,"WARNING: Source DB must be in the user-defined target resolution")
    print("ADDING CARGO TRANSPORT")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]

            if entity_names[0] in config["onshore_polygons"] and entity_names[-1] in config["onshore_polygons"] and config["user"]["cargo"][entity_names[1]]["status"] and config["user"][entity_class_elements[1]][entity_names[1]]["status"] and config["user"][entity_class_elements[1]][entity_names[1]]["node_type"] == "balance":               
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_items[2]])
                                if param_items[0] == "investment_method": # Particular Case Screening Out
                                    if not db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name="links_potentials",alternative_name="Base"):
                                        param_items[1] = "not_allowed"
                                add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                values_ = db_source.get_parameter_value_items(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source)
                                if values_:
                                    for value_ in values_:
                                        value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],value_["alternative_name"],entity_target_name,value_param)
          
def add_policy_constraints(db_map : DatabaseMapping, config : dict):

    co2_values = [config["user"]["global_constraints"]["co2_annual_budget"][year] for year in config["user"]["global_constraints"]["co2_annual_budget"]]
    co2_years  = [f"y{year}" for year in config["user"]["global_constraints"]["co2_annual_budget"]]
    co2_budget = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(co2_years,co2_values))}
    # Atmosphere entity is created
    entity_name = "set"
    entity_byname = ("atmosphere",)
    add_entity(db_map,entity_name,entity_byname)
    add_parameter_value(db_map,entity_name,"co2_max_cumulative","Base",entity_byname,co2_budget)

    # co2 storage entity is created
    if not config["user"]["commodity"]["CO2"]["status"]:
        entity_name = "node"
        entity_byname = ("CO2",)
        try:
            add_entity(db_map,entity_name,entity_byname)
        except:
            pass
        add_parameter_value(db_map,entity_name,"storage_investment_method","Base",entity_byname,"not_allowed")
        add_parameter_value(db_map,entity_name,"storage_retirement_method","Base",entity_byname,"not_retired")
        add_parameter_value(db_map,entity_name,"storage_state_fix_method","Base",entity_byname,"fix_start")
        add_parameter_value(db_map,entity_name,"storage_state_fix","Base",entity_byname,0.0)
        co2_storage = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(co2_years,[float(config["user"]["global_constraints"]["co2_annual_sequestration"]/1000) for _ in range(3)]))}
        add_parameter_value(db_map,entity_name,"storages_existing","Base",entity_byname,co2_storage)
        add_parameter_value(db_map,entity_name,"storage_capacity","Base",entity_byname,float(1000))
              
def main():

    url_db_out = sys.argv[1]
    url_db_com = sys.argv[2]
    url_db_pow = sys.argv[3]
    url_db_vre = sys.argv[4]
    url_db_tra = sys.argv[5]
    url_db_hyd = sys.argv[6]
    url_db_dem = sys.argv[7]
    url_db_ind = sys.argv[8]
    url_db_bio = sys.argv[9]
    url_db_gas = sys.argv[10]
    url_db_veh = sys.argv[11]
    url_db_hea = sys.argv[12]
    url_db_car = sys.argv[13]

    with open("ines_structure.json", 'r') as f:
        ines_spec = json.load(f)

    config = {"sys":yaml.safe_load(open("sysconfig.yaml", "rb")),"user":yaml.safe_load(open(sys.argv[14], "rb"))}
    config["transformer"] = pd.read_excel("region_transformation.xlsx",sheet_name=None)
    polygons = define_polygons(config["user"],config["transformer"])
    config["onshore_polygons"]  = polygons["onshore"]
    config["offshore_polygons"] = polygons["offshore"]   

    with DatabaseMapping(url_db_out) as db_map:

        # Importing Map
        api.import_data(db_map,
                    entity_classes=ines_spec["entity_classes"],
                    parameter_value_lists=ines_spec["parameter_value_lists"],
                    parameter_definitions=ines_spec["parameter_definitions"],
                    )
        add_superclass_subclass(db_map,"unit_flow","node__to_unit")
        add_superclass_subclass(db_map,"unit_flow","unit__to_node")
        print("ines_map_added")
        db_map.refresh_session()
        db_map.commit_session("ines_map_added")
        
        # Base alternative
        add_alternative(db_map,"Base")

        # Timeline Structure
        add_timeline(db_map,config)
        print("timeline_added")
        db_map.commit_session("timeline_added")

        # Power Sector Representation
        if config["user"]["pipelines"]["power"]:
            with DatabaseMapping(url_db_pow) as db_pow:
                db_pow.fetch_all()
                add_power_sector(db_map,db_pow,config)
                print("power_sector_added")
                db_map.commit_session("power_sector_added")

        # Hydro Systems
        if config["user"]["pipelines"]["hydro"]:
            with DatabaseMapping(url_db_hyd) as db_hyd:
                db_hyd.fetch_all()
                add_hydro(db_map,db_hyd,config)
                print("hydro_systems_added")
                db_map.commit_session("hydro_systems_added")
        
        # Power VRE Representation
        if config["user"]["pipelines"]["vre"]:
            with DatabaseMapping(url_db_vre) as db_vre:
                db_vre.fetch_all()
                add_vre_sector(db_map,db_vre,config)
                print("vre_added")
                db_map.commit_session("vre_added")

        # Power Transmission Representation
        if config["user"]["pipelines"]["electricity_transmission"]:
            with DatabaseMapping(url_db_tra) as db_tra:
                db_tra.fetch_all()
                add_power_transmission(db_map,db_tra,config)
                print("power_transmission_added")
                db_map.commit_session("power_transmission_added")
        
        # Electricity Demand
        if config["user"]["pipelines"]["residual_demand"]:
            with DatabaseMapping(url_db_dem) as db_dem:
                db_dem.fetch_all()
                add_electricity_demand(db_map,db_dem,config)
                print("electricity_demand_added")
                db_map.commit_session("electricity_demand_added")

        #  Industrial Sector
        if config["user"]["pipelines"]["industry"]:
            with DatabaseMapping(url_db_ind) as db_ind:
                db_ind.fetch_all()
                add_industrial_sector(db_map,db_ind,config)
                print("industrial_sector_added")
                db_map.commit_session("industrial_sector_added")

        #  Biomass Sector
        if config["user"]["pipelines"]["biomass"]:
            with DatabaseMapping(url_db_bio) as db_bio:
                db_bio.fetch_all()
                add_biomass_production(db_map,db_bio,config)
                print("biomass_sector_added")
                db_map.commit_session("biomass_sector_added")

        # Gas Sector Representation
        if config["user"]["pipelines"]["gas"]:
            with DatabaseMapping(url_db_gas) as db_gas:
                db_gas.fetch_all()
                add_gas_sector(db_map,db_gas,config)
                print("gas_sector_added")
                db_map.commit_session("gas_sector_added")
    
                if config["user"]["pipelines"]["gas_pipelines"]:
                    add_gas_pipelines(db_map,db_gas,config)
                    print("gas_pipelines_added")
                    db_map.commit_session("gas_pipelines_added")
        
        # Transport Representation
        if config["user"]["pipelines"]["transport"]:
            with DatabaseMapping(url_db_veh) as db_veh:
                db_veh.fetch_all()
                add_transport(db_map,db_veh,config)
                print("transport_added")
                db_map.commit_session("transport_added")

        # Heat Sector Representation
        if config["user"]["pipelines"]["heat"]:
            with DatabaseMapping(url_db_hea) as db_hea:
                db_hea.fetch_all()
                add_heat_sector(db_map,db_hea,config)
                print("heat_sector_added")
                db_map.commit_session("heat_sector_added")

        # Cargo Sector Representation
        if config["user"]["pipelines"]["cargo"]:
            with DatabaseMapping(url_db_car) as db_car:
                db_car.fetch_all()
                add_cargo_sector(db_map,db_car,config)
                print("cargo_sector_added")
                db_map.commit_session("cargo_sector_added")

        # Commodity Nodes parameters
        with DatabaseMapping(url_db_com) as db_com:
            db_com.fetch_all()
            add_nodes(db_map,db_com,config)
            print("nodes_added")
            db_map.commit_session("nodes_added")

        # Policy Constraints
        add_policy_constraints(db_map,config)
        print("policy_constraints")
        db_map.commit_session("policy_constraints")

if __name__ == "__main__":
    main()